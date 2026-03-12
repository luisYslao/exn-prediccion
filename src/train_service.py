from repository.spark_repository import create_spark, read_sql_table
from config.logging_config import logger
from config.settings import settings
from domain.schemas import DenegadasSchema, DenegadasTable, MlTrainSchema
from config.constants import (
    AGG_DEN_PATH, TRAIN_DEN_PATH, CURRENT_DEN_PATH, MODEL_DEN_PATH, PERU_HOLIDAYS
)
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, count, to_date, dayofweek, dayofmonth, weekofyear, year,
    month, when, date_sub, explode, sequence, day,
    lag, avg, lit, expr, min, trim, stddev, concat
)
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DateType, StringType
)
import pyarrow.parquet as pq
import lightgbm as lgb
import pandas as pd
import numpy as np
import os
import shutil
import gc
from datetime import datetime, timedelta


def load_data(spark):
    if os.path.exists(AGG_DEN_PATH):
        shutil.rmtree(AGG_DEN_PATH)

    execution_date = datetime.strptime(settings.EXECUTION_DAY, "%Y-%m-%d")
    start_date = execution_date - timedelta(days=settings.LAG_EXECUTION)

    sources = [
        f"Den{year}"
        for year in range(start_date.year, execution_date.year + 1)
    ]

    for den_tb in sources:
        logger.info(f"Loading denied table: {den_tb}")
        df_den = read_sql_table(
            spark,
            "Denegadas",
            f"[dbo].[{den_tb}]",
            f"{DenegadasTable.MCC}, {DenegadasTable.CODCOMERCIO}, {DenegadasTable.FECHAOP}"
        )
        logger.info(f"Total denied rows: {df_den.count()}")
        agregate_data(df_den)


def agregate_data(df_den):
    logger.info("Aggregating denied data")
    df_agg = (
        df_den
        .withColumn(DenegadasTable.CODCOMERCIO, trim(col(DenegadasTable.CODCOMERCIO)))
        .filter(col(DenegadasTable.CODCOMERCIO) != "0")
        .withColumn(DenegadasSchema.FECHA, to_date(col(DenegadasTable.FECHAOP)))
        .withColumn(
            DenegadasTable.MCC,
            when(
                col(DenegadasTable.MCC).isNull() | (trim(col(DenegadasTable.MCC)) == ""),
                lit("-1")
            ).otherwise(trim(col(DenegadasTable.MCC))).cast('int')
        )
        .groupBy(DenegadasTable.CODCOMERCIO, DenegadasSchema.FECHA, DenegadasTable.MCC)
        .agg(count("*").alias(DenegadasSchema.TOTAL))
    )
    logger.info(f"Total denied AGG: {df_agg.count()}")

    df_agg.write \
        .mode("append") \
        .partitionBy(DenegadasTable.MCC) \
        .parquet(AGG_DEN_PATH)


def build_data(spark):
    logger.info("Prepare data for denied")
    den_schema = StructType([
        StructField(MlTrainSchema.MCC, IntegerType(), False),
        StructField(MlTrainSchema.CODCOMERCIO, StringType(), False),
        StructField(MlTrainSchema.FECHA, DateType(), False),
        StructField(MlTrainSchema.TOTAL, LongType(), False),
    ])
    df_den = spark.read.schema(den_schema).parquet(AGG_DEN_PATH)
    logger.info(f"Total loaded denied: {df_den.count()}")
    build_ml(df_den, TRAIN_DEN_PATH, CURRENT_DEN_PATH)
    logger.info("End ml denied")


def build_ml(df, train_path, current_path):
    logger.info("Add ml columns")

    df = df.withColumn(MlTrainSchema.CODCOMERCIO, col(MlTrainSchema.CODCOMERCIO).cast('long'))

    logger.info("Generating calendar")
    df_calendar = (
        df
        .groupBy(MlTrainSchema.MCC, MlTrainSchema.CODCOMERCIO)
        .agg(min(col(MlTrainSchema.FECHA)).alias(MlTrainSchema.MIN_FECHA))
        .withColumn(
            MlTrainSchema.FECHA,
            explode(
                sequence(
                    col(MlTrainSchema.MIN_FECHA),
                    to_date(lit(settings.EXECUTION_DAY)),
                    expr("interval 1 day")
                )
            )
        )
        .drop(MlTrainSchema.MIN_FECHA)
    )

    df_dense = (
        df_calendar
        .join(df, on=[MlTrainSchema.MCC, MlTrainSchema.CODCOMERCIO, MlTrainSchema.FECHA], how="left")
        .fillna({MlTrainSchema.TOTAL: 0})
    )

    logger.info("Generating time fields")
    df_dense = (
        df_dense
        .withColumn(MlTrainSchema.DOW, dayofweek(col(MlTrainSchema.FECHA)))
        .withColumn(MlTrainSchema.WEEK, weekofyear(col(MlTrainSchema.FECHA)))
        .withColumn(MlTrainSchema.DAY, day(col(MlTrainSchema.FECHA)))
        .withColumn(MlTrainSchema.MONTH, month(col(MlTrainSchema.FECHA)))
        .withColumn(MlTrainSchema.YEAR, year(col(MlTrainSchema.FECHA)))
        .withColumn(MlTrainSchema.IS_WEEKEND, when(col(MlTrainSchema.DOW).isin(1, 7), 1).otherwise(0))
        .withColumn(
            MlTrainSchema.IS_HOLIDAY,
            when(
                concat(month(col(MlTrainSchema.FECHA)), lit("-"), dayofmonth(col(MlTrainSchema.FECHA)))
                .isin(PERU_HOLIDAYS), 1
            ).otherwise(0)
        )
        .withColumn(MlTrainSchema.IS_PAYDAY, when(dayofmonth(col(MlTrainSchema.FECHA)).isin(15, 30, 31), 1).otherwise(0))
        .withColumn(MlTrainSchema.IS_MONTH_START, when(dayofmonth(col(MlTrainSchema.FECHA)).isin(1, 2, 3), 1).otherwise(0))
        .withColumn(MlTrainSchema.IS_MONTH_END, when(dayofmonth(col(MlTrainSchema.FECHA)).isin(28, 29, 30, 31), 1).otherwise(0))
    )

    logger.info("Generating lag fields")
    w_mcc_com = Window.partitionBy(MlTrainSchema.MCC, MlTrainSchema.CODCOMERCIO).orderBy(MlTrainSchema.FECHA)
    w_mcc_co_7 = w_mcc_com.rowsBetween(-7, -1)
    w_mcc_co_14 = w_mcc_com.rowsBetween(-14, -1)

    df_dense = (
        df_dense
        .withColumn(MlTrainSchema.DEN_LAG_1, lag(MlTrainSchema.TOTAL, 1).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_LAG_2, lag(MlTrainSchema.TOTAL, 2).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_LAG_7, lag(MlTrainSchema.TOTAL, 7).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_LAG_14, lag(MlTrainSchema.TOTAL, 14).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_LAG_30, lag(MlTrainSchema.TOTAL, 30).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_MA_7, avg(MlTrainSchema.TOTAL).over(w_mcc_co_7))
        .withColumn(MlTrainSchema.DEN_MA_14, avg(MlTrainSchema.TOTAL).over(w_mcc_co_14))
        .withColumn(MlTrainSchema.DEN_STD_7, stddev(MlTrainSchema.TOTAL).over(w_mcc_co_7))
        .withColumn(MlTrainSchema.DIFF_WEEK, col(MlTrainSchema.DEN_LAG_1) - col(MlTrainSchema.DEN_LAG_7))
        .fillna(0)
        .orderBy(MlTrainSchema.FECHA)
    )

    logger.info("Save main parquet")
    df_dense.write \
        .mode("overwrite") \
        .partitionBy(MlTrainSchema.MCC) \
        .parquet(train_path)

    logger.info("Save minimal 60 parquet")
    (
        df_dense
        .filter(col(MlTrainSchema.FECHA) >= date_sub(lit(settings.EXECUTION_DAY), 60))
        .write
        .mode("overwrite")
        .partitionBy(MlTrainSchema.MCC)
        .parquet(current_path)
    )


def prepare_cluster():
    for mcc_path in os.listdir(TRAIN_DEN_PATH):
        if MlTrainSchema.MCC in mcc_path:
            logger.info(f"Prepare denied model for {mcc_path}")
            train_model_streaming(f"{TRAIN_DEN_PATH}/{mcc_path}", mcc_path)


def train_model_streaming(parquet_path, cluster):
    logger.info(f"Reading partitioned dataset: {parquet_path}")

    dataset = pq.ParquetDataset(parquet_path)
    estimated_rows = sum(f.metadata.num_rows for f in dataset.fragments)
    logger.info(f"Estimated rows: {estimated_rows}")

    fraction = 0.2 if estimated_rows >= 111_000_000 else 1.0
    sampled_batches = []

    for fragment in dataset.fragments:
        table = fragment.to_table(columns=MlTrainSchema.FEATURES + [MlTrainSchema.TOTAL])
        pdf_chunk = table.to_pandas()
        if len(pdf_chunk) > 0:
            sampled_batches.append(pdf_chunk.sample(frac=fraction, random_state=42))
        del table, pdf_chunk
        gc.collect()

    full_data = pd.concat(sampled_batches, ignore_index=True)
    del sampled_batches
    gc.collect()

    logger.info(f"Sample ready with {len(full_data)} rows. Training...")

    X = full_data[MlTrainSchema.FEATURES].to_numpy()
    y = full_data[MlTrainSchema.TOTAL].to_numpy()
    del full_data
    gc.collect()

    train_data = lgb.Dataset(
        X, label=y,
        feature_name=MlTrainSchema.FEATURES,
        categorical_feature=MlTrainSchema.CAT_FEATURE,
        free_raw_data=True
    )

    params = {
        "objective": "tweedie",
        "tweedie_variance_power": 1.2,
        "metric": ["rmse"],
        "learning_rate": 0.05,
        "num_leaves": 127,
        "min_data_in_leaf": 200,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 1,
        "lambda_l1": 0.05,
        "lambda_l2": 0.05,
        "verbose": -1,
        "num_threads": 1
    }

    model = lgb.train(params, train_data, num_boost_round=200)

    output_dir = f"{MODEL_DEN_PATH}"
    os.makedirs(output_dir, exist_ok=True)
    model.save_model(f"{output_dir}/model_{cluster}.txt")
    logger.info(f"Model saved: {output_dir}/model_{cluster}.txt")

    del X, y, train_data, model
    gc.collect()


def process():
    start_time = datetime.now()
    logger.info(f"Starting processing at {start_time:%Y-%m-%d %H:%M:%S}")

    logger.info("Creating Spark session...")
    spark = create_spark()

    logger.info("Loading Data")
    load_data(spark)

    logger.info("Build Data")
    build_data(spark)

    logger.info("Prepare Cluster")
    prepare_cluster()

    end_time = datetime.now()
    logger.info(f"Finished processing at {end_time:%Y-%m-%d %H:%M:%S}")
    logger.info(f"Total duration: {end_time - start_time}")


def main():
    try:
        process()
        logger.info("Training completed successfully.")
    except Exception as e:
        logger.error(f"Training failed: {e}")


if __name__ == "__main__":
    logger.info("Starting train service.")
    main()
