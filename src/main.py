from repository.spark_repository import create_spark, read_sql_table, read_sql_table_w
from config.constants import (
    AGG_DEN_PATH, AGG_TX_PATH, TRAIN_DEN_PATH, TRAIN_TX_PATH,
    CURRENT_DEN_PATH, CURRENT_TX_PATH, PERU_HOLIDAYS
)
from config.logging_config import logger
from config.settings import settings
from domain.schemas import DenegadasSchema, DenegadasTable, MlTrainSchema, TransaccionesSchema, TransaccionesTable
from datetime import datetime, date
from pyspark.sql.functions import (
    col, count, to_date, dayofweek, dayofmonth, weekofyear, year, 
    month, when, date_sub, explode, sequence, day,
    lag, avg, lit, to_date, expr, min, trim, stddev,
    row_number, concat, coalesce
)
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DateType, StringType
)
import pyarrow.parquet as pq
import lightgbm as lgb
import numpy as np
import os
from azure.storage.queue import QueueClient, TextBase64EncodePolicy
import json
from functools import reduce
import gc
import pandas as pd


def load_data(spark):
    sources = [
        (
            f"Den{year}",                     # Denegadas Tabla
            f"Year{year}",                    # Transacciones BD
            f"Año{year}",                     # Transacciones Tabla
            year >= settings.DOUBLE_FROM_YEAR # Es doble Transaccion DB Tabla
        )
        for year in range(settings.START_YEAR, settings.END_YEAR + 1 )
    ]
   
    for denTb, txDb, txTb, IsDouble in sources:
        logger.info(f"Reading : {denTb}, {txDb}, {txTb}, {IsDouble}")

        logger.info(f"Loading denied... Table: {denTb}")
        df_den = read_sql_table(
            spark,
            "Denegadas",
            f"[dbo].[{denTb}]",
            f"{DenegadasTable.MCC}, {DenegadasTable.CODCOMERCIO}, {DenegadasTable.FECHAOP}"
        )
        logger.info(f"Total denied: {df_den.count()}")

        #logger.info(f"Loading transactions... Db: {txDb}, Table: {txTb}")
        #df_tx = read_sql_table(
        #    spark,
        #    f"{txDb}",
        #    f"[dbo].[{txTb}]",
        #    f"{TransaccionesTable.MCC},{TransaccionesTable.CODCOMERCIO}, {TransaccionesTable.FECHAOPERACION}"
        #)
        #logger.info(f"Total transactions: {df_tx.count()}")

        #if IsDouble:
        #    logger.info(f"Loading transactions 2... Db: {txDb}-2, Table: {txTb}-2")
        #    df_tx2 = read_sql_table(
        #        spark,
        #        f"{txDb}-2",
        #        f"[dbo].[{txTb}-2]",
        #        f"{TransaccionesTable.MCC},{TransaccionesTable.CODCOMERCIO}, {TransaccionesTable.FECHAOPERACION}"
        #    )
        #    logger.info(f"Total transactions 2: {df_tx2.count()}")
        #    df_tx = df_tx.unionByName(df_tx2)
        #    logger.info(f"Total transactions Union: {df_tx.count()}")

        agregate_data(df_den)#, df_tx)


def agregate_data(df_den):# , df_tx):
    logger.info("Making agregation data")
    df_den_agregated = (
        df_den
        .withColumn(
            DenegadasTable.CODCOMERCIO,
            trim(col(DenegadasTable.CODCOMERCIO))
        )
        .filter(col(DenegadasTable.CODCOMERCIO) != "0")
        .withColumn(
            DenegadasSchema.FECHA,
            to_date(col(DenegadasTable.FECHAOP))
        )
        .withColumn(
            DenegadasTable.MCC,
            when(
                col(DenegadasTable.MCC).isNull() |
                (trim(col(DenegadasTable.MCC)) == ""),
                lit("-1")
            ).otherwise(trim(col(DenegadasTable.MCC))).cast('int')
        )
        .groupBy(
            DenegadasTable.CODCOMERCIO,
            DenegadasSchema.FECHA,
            DenegadasTable.MCC
        )
        .agg(
            count("*").alias(DenegadasSchema.TOTAL)
        )
    )
    logger.info(f"Total denied AGG: {df_den_agregated.count()}")

    df_den_agregated.write \
        .mode("append") \
        .partitionBy(DenegadasTable.MCC) \
        .parquet(AGG_DEN_PATH)

    #df_tx_agregated = (
    #    df_tx
    #    .withColumn(
    #        TransaccionesTable.CODCOMERCIO,
    #        trim(col(TransaccionesTable.CODCOMERCIO))
    #    )
    #    .filter(col(TransaccionesTable.CODCOMERCIO) != "0")
    #    .withColumn(
    #        TransaccionesSchema.FECHA,
    #        to_date(col(TransaccionesTable.FECHAOPERACION))
    #    )
    #    .withColumn(
    #        TransaccionesTable.MCC,
    #        when(
    #            col(TransaccionesTable.MCC).isNull() |
    #            (trim(col(TransaccionesTable.MCC)) == ""),
    #            lit("-1")
    #        ).otherwise(trim(col(TransaccionesTable.MCC))).cast('int')
    #    )
    #    .groupBy(
    #        TransaccionesTable.CODCOMERCIO,
    #        TransaccionesSchema.FECHA,
    #        TransaccionesTable.MCC
    #    )
    #    .agg(
    #        count("*").alias(TransaccionesSchema.TOTAL)
    #    )
    #)
    #logger.info(f"Total transactions AGG: {df_tx_agregated.count()}")

    #df_tx_agregated.write \
    #    .mode("append") \
    #    .partitionBy(TransaccionesSchema.MCC) \
    #    .parquet(AGG_TX_PATH)


def build_data(spark):
    logger.info("Prepare data for denied")
    den_schema = StructType([
        StructField(MlTrainSchema.MCC, IntegerType(), False),
        StructField(MlTrainSchema.CODCOMERCIO, StringType(), False),
        StructField(MlTrainSchema.FECHA, DateType(), False),
        StructField(MlTrainSchema.TOTAL, LongType(), False),
    ])
    df_den = (
        spark.read
        .schema(den_schema)
        .parquet(AGG_DEN_PATH)
    )
    logger.info(f"Total loaded denied: {df_den.count()}")
    build_ml(df_den, TRAIN_DEN_PATH, CURRENT_DEN_PATH)
    logger.info("End ml denied")

    #logger.info("Prepare data for transactions")
    #tx_schema = StructType([
    #    StructField(MlTrainSchema.MCC, IntegerType(), False),
    #    StructField(MlTrainSchema.CODCOMERCIO, StringType(), False),
    #    StructField(MlTrainSchema.FECHA, DateType(), False),
    #    StructField(MlTrainSchema.TOTAL, LongType(), False),
    #])
    #df_tx = (
    #    spark.read
    #    .schema(tx_schema)
    #    .parquet(AGG_TX_PATH)
    #)
    #logger.info(f"total loaded transactions: {df_tx.count()}")
    #build_ml(df_tx, TRAIN_TX_PATH, CURRENT_TX_PATH)
    #logger.info("End ml transactions")


def build_ml(df, train_path, current_path):
    logger.info("Add ml columns")

    df = df.withColumn(MlTrainSchema.CODCOMERCIO, col(MlTrainSchema.CODCOMERCIO).cast('long'))

    logger.info("Generating calendar")  

    df_calendar = (
        df
        .groupBy(
            MlTrainSchema.MCC,
            MlTrainSchema.CODCOMERCIO
        )
        .agg(
            min(col(MlTrainSchema.FECHA)).alias(MlTrainSchema.MIN_FECHA)
        )
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
        .join(
            df,
            on=[
                MlTrainSchema.MCC,
                MlTrainSchema.CODCOMERCIO,
                MlTrainSchema.FECHA
            ],
            how="left"
        )
        .fillna({MlTrainSchema.TOTAL: 0})
    )

    logger.info("Generating time fields") 
    
    df_dense = (
        df_dense.withColumn(MlTrainSchema.DOW, dayofweek(col(MlTrainSchema.FECHA)))
            .withColumn(MlTrainSchema.WEEK, weekofyear(col(MlTrainSchema.FECHA)))
            .withColumn(MlTrainSchema.DAY, day(col(MlTrainSchema.FECHA)))
            .withColumn(MlTrainSchema.MONTH, month(col(MlTrainSchema.FECHA)))
            .withColumn(MlTrainSchema.YEAR, year(col(MlTrainSchema.FECHA)))
            .withColumn(
                MlTrainSchema.IS_WEEKEND,
                when(col(MlTrainSchema.DOW).isin(1, 7), 1).otherwise(0)
            )
            .withColumn(
                MlTrainSchema.IS_HOLIDAY,
                when(
                    concat(month(col(MlTrainSchema.FECHA)), lit("-"), dayofmonth(col(MlTrainSchema.FECHA)))
                    .isin(PERU_HOLIDAYS), 1
                ).otherwise(0)
            )
            .withColumn(
                MlTrainSchema.IS_PAYDAY,
                when(dayofmonth(col(MlTrainSchema.FECHA)).isin(15, 30, 31), 1).otherwise(0)
            )
            .withColumn(
                MlTrainSchema.IS_MONTH_START,
                when(dayofmonth(col(MlTrainSchema.FECHA)).isin(1, 2, 3), 1).otherwise(0)
            )
            .withColumn(
                MlTrainSchema.IS_MONTH_END,
                when(dayofmonth(col(MlTrainSchema.FECHA)).isin(28, 29, 30, 31), 1).otherwise(0)
            )
    )

    logger.info("Generating lag fields") 
    w_mcc_com = Window.partitionBy(
        MlTrainSchema.MCC,
        MlTrainSchema.CODCOMERCIO
    ).orderBy(MlTrainSchema.FECHA)
    w_mcc_co_7 = w_mcc_com.rowsBetween(-7, -1)
    w_mcc_co_14 = w_mcc_com.rowsBetween(-14, -1)

    df_dense = ( 
        df_dense.withColumn(MlTrainSchema.DEN_LAG_1, lag(MlTrainSchema.TOTAL, 1).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_LAG_2, lag(MlTrainSchema.TOTAL, 2).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_LAG_7, lag(MlTrainSchema.TOTAL, 7).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_LAG_14, lag(MlTrainSchema.TOTAL, 14).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_LAG_30, lag(MlTrainSchema.TOTAL, 30).over(w_mcc_com))
        .withColumn(
            MlTrainSchema.DEN_MA_7,
            avg(MlTrainSchema.TOTAL).over(w_mcc_co_7)
        )
        .withColumn(
            MlTrainSchema.DEN_MA_14,
            avg(MlTrainSchema.TOTAL).over(w_mcc_co_14)
        )
        .withColumn(MlTrainSchema.DEN_STD_7, stddev(MlTrainSchema.TOTAL).over(w_mcc_co_7))
        .withColumn(MlTrainSchema.DIFF_WEEK, col(MlTrainSchema.DEN_LAG_1) - col(MlTrainSchema.DEN_LAG_7))
        .fillna(0)
        .orderBy(MlTrainSchema.FECHA)
    )    

    logger.info("Save main parquet")  
    # df_dense.write \
    #     .mode("overwrite") \
    #     .partitionBy(MlTrainSchema.MCC) \
    #     .parquet(train_path)
    
    df_last_60 = (
        df_dense
        .filter(
            col(MlTrainSchema.FECHA) >= date_sub(lit(settings.EXECUTION_DAY), 60)
        )
    )
    logger.info("Save minimal 60 parquet") 
    df_last_60.write \
        .mode("overwrite") \
        .partitionBy(MlTrainSchema.MCC) \
        .parquet(current_path)


def prepare_cluster():
    for mcc_path in os.listdir(TRAIN_DEN_PATH):
        if(MlTrainSchema.MCC in mcc_path):
            logger.info(f"Prepare denied model for {mcc_path}")
            #data = pq.read_table(f"{TRAIN_DEN_PATH}/{mcc_path}")
            train_model_streaming(f"{TRAIN_DEN_PATH}/{mcc_path}", mcc_path, "denied")
    #for mcc_path in os.listdir(TRAIN_TX_PATH):
    #    if(MlTrainSchema.MCC in mcc_path):
    #       logger.info(f"Prepare transactions model for {mcc_path}")
    #       data = pq.read_table(f"{TRAIN_TX_PATH}/{mcc_path}")
    #       train_model(data, mcc_path, "transactions")


def train_model_streaming(parquet_path, cluster, type):
    logger.info(f"Leyendo dataset particionado en: {parquet_path}")
    
    # 1. Usamos ParquetDataset para que entienda las particiones MCC
    dataset = pq.ParquetDataset(parquet_path)
    sampled_batches = []

    estimated_rows = sum(f.metadata.num_rows for f in dataset.fragments)
    logger.info(f"Estimated rows: {estimated_rows}")
    # Iteramos sobre cada archivo '.part' individualmente

    fraction = 0.2 if estimated_rows >= 111_000_000 else 1.0
    for fragment in dataset.fragments:
        # Leemos el fragmento y lo convertimos a pandas
        # Nota: Pyarrow cargará la columna MCC automáticamente si es parte de la ruta
        table = fragment.to_table(columns=MlTrainSchema.FEATURES + [MlTrainSchema.TOTAL])
        pdf_chunk = table.to_pandas()
        
        if len(pdf_chunk) > 0:
            # Tomamos el 10% de cada parte para mantener la representatividad
            sample_chunk = pdf_chunk.sample(frac= fraction, random_state=42)
            sampled_batches.append(sample_chunk)
        
        # Limpieza agresiva de memoria por cada fragmento
        del table, pdf_chunk
        gc.collect()

    # Unimos todas las muestras pequeñas en un DataFrame final
    full_data = pd.concat(sampled_batches, ignore_index=True)
    del sampled_batches
    gc.collect()

    logger.info(f"Muestra lista con {len(full_data)} filas. Entrenando...")

    # 2. Preparar matrices en float32 (ahorra 50% RAM)
    X = full_data[MlTrainSchema.FEATURES].to_numpy()
    y = full_data[MlTrainSchema.TOTAL].to_numpy()
    del full_data
    gc.collect()

    # 3. Entrenamiento con tus parámetros de 63 hojas pero optimizados
    train_data = lgb.Dataset(X, label=y, feature_name= MlTrainSchema.FEATURES, categorical_feature= MlTrainSchema.CAT_FEATURE, free_raw_data=True)

    params2 = {
        "objective": "tweedie",
        "metric": ["rmse"],
        "learning_rate": 0.07,
        "num_leaves": 63,
        "max_bin": 63,              # CRÍTICO para no saturar RAM
        "min_data_in_leaf": 500,    # Ajustado para el volumen de 150k
        "data_sample_strategy": "goss",
        "feature_fraction": 0.7,
        "num_threads": 1,
        "verbose": -1,
    }

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

    # 4. Guardar
    output_dir = f"tmp/models/{type}"
    os.makedirs(output_dir, exist_ok=True)
    model.save_model(f"{output_dir}/model_{cluster}.txt")
    logger.info("Modelo de gran escala guardado con éxito.")
    
    del X, y, train_data, model
    gc.collect()


def train_model(data, cluster, type):
    X = np.column_stack([data.column(c).to_numpy() for c in MlTrainSchema.FEATURES])
    y = data.column(MlTrainSchema.TOTAL).to_numpy()

    del data
    gc.collect()
    logger.info("cleaned data")
    train_data = lgb.Dataset(
        X, label=y, feature_name= MlTrainSchema.FEATURES, categorical_feature= MlTrainSchema.CAT_FEATURE,
        free_raw_data=True
    )

    del X
    del y
    gc.collect()
    logger.info("cleaned x and y ")

    model1k = lgb.train(
        {
            "objective": "tweedie",
            "tweedie_variance_power": 1.2,
            "metric": ["rmse"],
            "learning_rate": 0.07,
            "num_leaves": 63,
            "min_data_in_leaf": 5,
            "feature_fraction": 1.0,
            "bagging_fraction": 0.9,
            "bagging_freq": 1,
            "lambda_l1": 0.00,
            "lambda_l2": 0.00,
            "verbose": -1,
            "num_threads": 1
        },
        train_data,
        num_boost_round=200,
    )

    model = lgb.train(
        {
            "objective": "tweedie",
            "tweedie_variance_power": 1.2,
            "metric": ["rmse"],
            "learning_rate": 0.1,
            "num_leaves": 31,
            "min_data_in_leaf": 10000,
            "max_bin": 15,
            "data_sample_strategy": "goss",
            "feature_fraction": 0.5,
            "lambda_l1": 0.00,
            "lambda_l2": 0.00,
            "two_round": True,
            "num_threads": 1,
            "verbose": -1,
        },
        train_data,
        num_boost_round=1,
    )

    logger.info("save model")
    output_dir = f"tmp/models/{type}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    model.save_model(f"tmp/models/{type}/model_{cluster}.txt")

    del train_data
    gc.collect()
    logger.info("cleaned data")


def process():
    '''Processing...'''
    start_time = datetime.now()
    logger.info(f"Starting processing at {start_time:%Y-%m-%d %H:%M:%S}")

    logger.info("Creating Spark session...")
    spark = create_spark()

    # To enable detailed logging, uncomment the following line
    # spark.sparkContext.setLogLevel("DEBUG")

    logger.info("Loading Data")
    load_data(spark)

    logger.info("Build Data")
    build_data(spark)

    logger.info("Prepare Cluster")
    #prepare_cluster()
    #prepare_cluster_unique()

    #predict_next_day()
    #predict(spark)
    #etl(spark)
    #etl2(spark)

    end_time = datetime.now()
    logger.info(f"Finished processing at {end_time:%Y-%m-%d %H:%M:%S}")
    logger.info(f"Total duration: {end_time - start_time}")


def predict(spark):
    model_path = "tmp/models/denied/model_MCC=6300.txt"
    data_path = f"{CURRENT_DEN_PATH}/MCC=6300"
    p0 = predict_next_day(spark, date(2025, 11, 30), model_path, data_path )
    p1 = predict_next_day(spark, date(2025, 12, 1), model_path, data_path )
    p2 = predict_next_day(spark, date(2025, 12, 2), model_path, data_path )
    p3 = predict_next_day(spark, date(2025, 12, 3), model_path, data_path )
    p4 = predict_next_day(spark, date(2025, 12, 4), model_path, data_path )
    p5 = predict_next_day(spark, date(2025, 12, 5), model_path, data_path )
    p6 = predict_next_day(spark, date(2025, 12, 6), model_path, data_path )
    p7 = predict_next_day(spark, date(2025, 12, 7), model_path, data_path )
    p8 = predict_next_day(spark, date(2025, 12, 8), model_path, data_path )
    p9 = predict_next_day(spark, date(2025, 12, 9), model_path, data_path )
    p10 = predict_next_day(spark, date(2025, 12, 10), model_path, data_path )
    p11 = predict_next_day(spark, date(2025, 12, 11), model_path, data_path )
    p12 = predict_next_day(spark, date(2025, 12, 12), model_path, data_path )
    p13 = predict_next_day(spark, date(2025, 12, 13), model_path, data_path )
    p14 = predict_next_day(spark, date(2025, 12, 14), model_path, data_path )
    p15 = predict_next_day(spark, date(2025, 12, 15), model_path, data_path )
    p16 = predict_next_day(spark, date(2025, 12, 16), model_path, data_path )
    p17 = predict_next_day(spark, date(2025, 12, 17), model_path, data_path )
    p18 = predict_next_day(spark, date(2025, 12, 18), model_path, data_path )
    p19 = predict_next_day(spark, date(2025, 12, 19), model_path, data_path )
    p20 = predict_next_day(spark, date(2025, 12, 20), model_path, data_path )
    p21 = predict_next_day(spark, date(2025, 12, 21), model_path, data_path )
    p22 = predict_next_day(spark, date(2025, 12, 22), model_path, data_path )
    p23 = predict_next_day(spark, date(2025, 12, 23), model_path, data_path )
    p24 = predict_next_day(spark, date(2025, 12, 24), model_path, data_path )
    p25 = predict_next_day(spark, date(2025, 12, 25), model_path, data_path )
    p26 = predict_next_day(spark, date(2025, 12, 26), model_path, data_path )
    p27 = predict_next_day(spark, date(2025, 12, 27), model_path, data_path )
    p28 = predict_next_day(spark, date(2025, 12, 28), model_path, data_path )
    p29 = predict_next_day(spark, date(2025, 12, 29), model_path, data_path )
    p30 = predict_next_day(spark, date(2025, 12, 30), model_path, data_path )
    p31 = predict_next_day(spark, date(2025, 12, 31), model_path, data_path )
    p11 = predict_next_day(spark, date(2026, 1, 1), model_path, data_path )


    logger.info(f"30/11 p= {p0} r=165")
    logger.info(f"01/12 p= {p1} r=81")
    logger.info(f"02/12 p= {p2} r=73")
    logger.info(f"03/12 p= {p3} r=71")
    logger.info(f"04/12 p= {p4} r=70")
    logger.info(f"05/12 p= {p5} r=67")
    logger.info(f"06/12 p= {p6} r=63")
    logger.info(f"07/12 p= {p7} r=60")
    logger.info(f"08/12 p= {p8} r=60")
    logger.info(f"09/12 p= {p9} r=60")
    logger.info(f"10/12 p= {p10} r=60")
    logger.info(f"11/12 p= {p11} r=60")
    logger.info(f"12/12 p= {p12} r=57")
    logger.info(f"13/12 p= {p13} r=53")
    logger.info(f"14/12 p= {p14} r=53")
    logger.info(f"15/12 p= {p15} r=104")
    logger.info(f"16/12 p= {p16} r=50")
    logger.info(f"17/12 p= {p17} r=47")
    logger.info(f"18/12 p= {p18} r=43")
    logger.info(f"19/12 p= {p19} r=43")
    logger.info(f"20/12 p= {p20} r=43")
    logger.info(f"21/12 p= {p21} r=43")
    logger.info(f"22/12 p= {p22} r=110")
    logger.info(f"23/12 p= {p23} r=104")
    logger.info(f"24/12 p= {p24} r=99")
    logger.info(f"25/12 p= {p25} r=194")
    logger.info(f"26/12 p= {p26} r=191")
    logger.info(f"27/12 p= {p27} r=191")
    logger.info(f"28/12 p= {p28} r=191")
    logger.info(f"29/12 p= {p29} r=191")
    logger.info(f"30/12 p= {p30} r=191")
    logger.info(f"30/12 p= {p31} r=191")


def predict_next_day(spark, date, model_path, data_path):
    df_data = (
        spark.read
        .parquet(data_path)
    )
    df_data = (
        df_data.filter(col(MlTrainSchema.FECHA) < date)
        .filter(col(MlTrainSchema.CODCOMERCIO) == lit("4716352"))
        .withColumn(MlTrainSchema.MCC, lit(6300))
    )

    df_last = (
        df_data
        .filter(col(MlTrainSchema.FECHA) == date_sub(lit(date), 1))
    )

    df_predict = (
        df_last
        .withColumn(MlTrainSchema.TOTAL, lit(0))
        .withColumn(MlTrainSchema.FECHA, lit(date))
        .withColumn(MlTrainSchema.DOW, dayofweek(lit(date)))
        .withColumn(MlTrainSchema.WEEK, weekofyear(lit(date)))
        .withColumn(MlTrainSchema.DAY, day(lit(date)))
        .withColumn(MlTrainSchema.MONTH, month(lit(date)))
        .withColumn(MlTrainSchema.YEAR, year(lit(date)))
        .withColumn(
            MlTrainSchema.IS_WEEKEND,
            when(dayofweek(lit(date)).isin(1, 7), 1).otherwise(0)
        )
        .withColumn(
            MlTrainSchema.IS_HOLIDAY,
            when(
                concat(month(col(MlTrainSchema.FECHA)), lit("-"), dayofmonth(col(MlTrainSchema.FECHA)))
                .isin(PERU_HOLIDAYS), 1
            ).otherwise(0)
        )
        .withColumn(
            MlTrainSchema.IS_PAYDAY,
            when(dayofmonth(col(MlTrainSchema.FECHA)).isin(15, 30, 31), 1).otherwise(0)
        )
        .withColumn(
            MlTrainSchema.IS_MONTH_START,
            when(dayofmonth(col(MlTrainSchema.FECHA)).isin(1, 2, 3), 1).otherwise(0)
        )
        .withColumn(
            MlTrainSchema.IS_MONTH_END,
            when(dayofmonth(col(MlTrainSchema.FECHA)).isin(28, 29, 30, 31), 1).otherwise(0)
        )
        
    )

    df_predict = df_data.unionByName(df_predict)

    w_mcc_com = Window.partitionBy(
        MlTrainSchema.MCC,
        MlTrainSchema.CODCOMERCIO
    ).orderBy(MlTrainSchema.FECHA)
    # w_mcc_co_4 = w_mcc_com.rowsBetween(-4, -1)
    w_mcc_co_7 = w_mcc_com.rowsBetween(-7, -1)
    w_mcc_co_14 = w_mcc_com.rowsBetween(-14, -1)
    w_mcc_co_30 = w_mcc_com.rowsBetween(-30, -1)

    df_predict = ( 
        df_predict.withColumn(MlTrainSchema.DEN_LAG_1, lag(MlTrainSchema.TOTAL, 1).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_LAG_2, lag(MlTrainSchema.TOTAL, 2).over(w_mcc_com))
        #.withColumn(MlTrainSchema.DEN_LAG_4, lag(MlTrainSchema.TOTAL, 4).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_LAG_7, lag(MlTrainSchema.TOTAL, 7).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_LAG_14, lag(MlTrainSchema.TOTAL, 14).over(w_mcc_com))
        .withColumn(MlTrainSchema.DEN_LAG_30, lag(MlTrainSchema.TOTAL, 30).over(w_mcc_com))
        #.withColumn(MlTrainSchema.DEN_LAG_180, lag(MlTrainSchema.TOTAL, 180).over(w_mcc_com))
        #.withColumn(MlTrainSchema.DEN_LAG_365, lag(MlTrainSchema.TOTAL, 365).over(w_mcc_com))
        # .withColumn(
        #     MlTrainSchema.DEN_MA_4,
        #     avg(MlTrainSchema.TOTAL).over(w_mcc_co_4)
        # )
        .withColumn(
            MlTrainSchema.DEN_MA_7,
            avg(MlTrainSchema.TOTAL).over(w_mcc_co_7)
        )
        .withColumn(
            MlTrainSchema.DEN_MA_14,
            avg(MlTrainSchema.TOTAL).over(w_mcc_co_14)
        )
        #.withColumn(
        #   MlTrainSchema.DEN_MA_30,
        #    avg(MlTrainSchema.TOTAL).over(w_mcc_co_30)
        #)
        #.withColumn(MlTrainSchema.DEN_RATIO_L1_MA7, col(MlTrainSchema.DEN_LAG_1) / (col(MlTrainSchema.DEN_MA_7) + lit(1.0)))
        .withColumn(MlTrainSchema.DEN_STD_7, stddev(MlTrainSchema.TOTAL).over(w_mcc_co_7))
        .withColumn(MlTrainSchema.DIFF_WEEK, col(MlTrainSchema.DEN_LAG_1) - col(MlTrainSchema.DEN_LAG_7))
        .fillna(0)
        .orderBy(MlTrainSchema.FECHA)
    )    

    rows = df_predict.filter(col(MlTrainSchema.FECHA) == date).select(*MlTrainSchema.FEATURES).collect()
    X = [list(row) for row in rows]
    model = lgb.Booster(model_file=model_path)
    logger.info("data loaded")

    prediction = model.predict(np.array(X))

    print(prediction)
    return float(prediction[0])
    #TEST 1   
    #si-96.92 - 95.64 - 88|6300|    4716352|2025-12-24|   99|  4|  52| 24|   12|2025|         0|      104|       47|        60|       107| 61.857142857142854| 62.142857142857146|  89.53333333333333|
    #si-178.73  -  231.05 - 128.78|6300|    4716352|2025-12-25|  194|  5|  52| 25|   12|2025|         0|       99|       43|        60|       208|  69.28571428571429|  64.92857142857143|  89.26666666666667|
    #si??-76.68 - 63.05 - 46.55 |6300|    4716352|2025-12-18|   43|  5|  51| 18|   12|2025|         0|       47|       60|        70|        48|  60.57142857142857| 61.714285714285715|  87.86666666666666|
    #idk??-182.81 157|6300|    191|2025-12-26|    0|  6|  52| 26|   12|2025|         0|      194|       43|        57|       200|  90.85714285714286|               74.5|               88.8| 
    #si-180  -  251.38 - 198.63|6300|    czzzz|2025-11-30|  165|  1|  48| 30|   11|2025|         1|      167|       50|        53|       185| 156.57142857142858| 106.21428571428571|               92.6|
    

def predict_next_day2():
    model = lgb.Booster(
        #model_file=f"tmp/models/_transactions/model_unique.txt"
        model_file=f"tmp/models/denied/model_MCC=6300.txt"
    )

    den_lag_1 = 99
    #den_lag_4 = 200
    den_lag_7 = 43
    den_lag_14 = 60
    den_lag_30 = 208
    #den_lag_180 = 66
    #den_lag_365 = 133
    #den_ma_4 = 182.75
    den_ma_7 = 69.28571428571429
    den_ma_14 = 64.92857142857143
    den_ma_30 = 89.26666666666667

    dow = 5 # next_date.isoweekday()
    week = 52 # next_date.isocalendar().week
    day = 25
    month = 12 # next_date.month
    year = 2025
    is_weekend = 0 # 1 if dow in (6, 7) else 0

#TEST 1   
#si-96.92|6300|    4716352|2025-12-24|   99|  4|  52| 24|   12|2025|         0|      104|       47|        60|       107| 61.857142857142854| 62.142857142857146|  89.53333333333333|
#si-178.73|6300|    4716352|2025-12-25|  194|  5|  52| 25|   12|2025|         0|       99|       43|        60|       208|  69.28571428571429|  64.92857142857143|  89.26666666666667|


#Test 5 F
#idk??-84.14|6300|    4716352|2025-12-24|   99|  4|   12|         0|      104|       47|        60| 61.857142857142854|
#idk??-84.43.61|6300|    4716352|2025-12-25|  194|  5|   12|         0|       99|       43|        60|  69.28571428571429|


#Test 4 F
#idk??-87.01|6300|    4716352|2025-12-24|   99|  4|   12|         0|      104|       47|        60| 61.857142857142854|
#idk??-87.61|6300|    4716352|2025-12-25|  194|  5|   12|         0|       99|       43|        60|  69.28571428571429|


#Test 3 F
#idk??-69.32|6300|    4716352|2025-12-24|   99|  4|   12|         0|      104|       47|        60| 61.857142857142854|
#idk??-69.78|6300|    4716352|2025-12-25|  194|  5|   12|         0|       99|       43|        60|  69.28571428571429|



#Test 2 F
#idk??-84.46|6300|    4716352|2025-12-24|   99|  4|  52| 24|   12|2025|         0|      104|       43|       47|        60|       107|        252|         78|    75.0| 61.857142857142854| 62.142857142857146|  89.53333333333333|
#idk??-147.55|6300|    4716352|2025-12-25|  194|  5|  52| 25|   12|2025|         0|       99|       43|       43|        60|       208|        165|        150|    89.0|  69.28571428571429|  64.92857142857143|  89.26666666666667|
#si-48.21|6300|    4716352|2025-12-18|   43|  5|  51| 18|   12|2025|         0|       47|       53|       60|        70|        48|         37|         34|    63.5|  60.57142857142857| 61.714285714285715|  87.86666666666666|
#idk??-86.92|6300|    4716352|2025-11-30|  165|  1|  48| 30|   11|2025|         1|      167|      200|       50|        53|       185|         66|        133|  182.75| 156.57142857142858| 106.21428571428571|               92.6|


#TEST 1   
#si-96.92|6300|    4716352|2025-12-24|   99|  4|  52| 24|   12|2025|         0|      104|       47|        60|       107| 61.857142857142854| 62.142857142857146|  89.53333333333333|
#si-178.73|6300|    4716352|2025-12-25|  194|  5|  52| 25|   12|2025|         0|       99|       43|        60|       208|  69.28571428571429|  64.92857142857143|  89.26666666666667|
#si??-76.68|6300|    4716352|2025-12-18|   43|  5|  51| 18|   12|2025|         0|       47|       60|        70|        48|  60.57142857142857| 61.714285714285715|  87.86666666666666|
#idk??-182.81|6300|    4716352|2025-12-26|    0|  6|  52| 26|   12|2025|         0|      194|       43|        57|       200|  90.85714285714286|               74.5|               88.8| 
#si-180|6300|    4716352|2025-11-30|  165|  1|  48| 30|   11|2025|         1|      167|       50|        53|       185| 156.57142857142858| 106.21428571428571|               92.6|

    X = np.array([[
        #"4816",
        #"4716352",
        dow,
        week,
        day,
        month,
        year,
        is_weekend,
        den_lag_1,
        #den_lag_4,
        den_lag_7,
        den_lag_14,
        den_lag_30,
        #den_lag_180,
        #den_lag_365,
        #den_ma_4,
        den_ma_7,
        den_ma_14,
        den_ma_30,
        "4716352"
    ]])

    # 5️⃣ predecir
    prediction = model.predict(X)
    print(prediction)
    return float(prediction[0])


def send_queue_mail():
    queue_client = QueueClient.from_connection_string(
        conn_str=settings.QUEUE_CN,
        queue_name=settings.QUEUE_NAME,
        message_encode_policy=TextBase64EncodePolicy()
    )
    mensaje = {
        "notification_id": 0,
        "to": [
            "jhon.concepcion@devmatte.com",
            "luis.yslao@devmatte.com",
            "dev.expressnet@devmatte.com"
        ],
        "cc": [],
        "template_id": "10013",
        "template_data": {
            "fileName": "Prueba Server",
            "error": "Prueba Server"
        },
        "attachments": []
    }

    queue_client.send_message(json.dumps(mensaje))


def notify(spark):
    df_den = (
        spark.read
        .parquet(CURRENT_DEN_PATH)
    )
    df_den = ( df_den
        .drop(MlTrainSchema.DEN_LAG_1)
        .drop("DEN_LAG_2")
        .drop(MlTrainSchema.DEN_LAG_7)
        .drop("DEN_LAG_14")
        .drop("DEN_LAG_30")
        .drop("DEN_MA_14")
        .drop("DEN_MA_30")
        .drop("DEN_STD_7")
        .drop("DIFF_WEEK")
        .drop(MlTrainSchema.DEN_MA_7)
    )
    w_mcc_com = Window.partitionBy(
        MlTrainSchema.MCC,
        MlTrainSchema.CODCOMERCIO
    ).orderBy(MlTrainSchema.FECHA)
    df_ma = (
        df_den
        .filter(col(MlTrainSchema.FECHA) < lit(settings.EXECUTION_DAY))
        .filter(col(MlTrainSchema.CODCOMERCIO) == lit("4716352"))
        .withColumn("DEN_MA_60",avg(MlTrainSchema.TOTAL).over(w_mcc_com))
        .orderBy(col(MlTrainSchema.FECHA).desc())
    )

    df_ma.show(1000)
    df_den.filter(col(MlTrainSchema.FECHA) == lit(settings.EXECUTION_DAY)).filter(col(MlTrainSchema.CODCOMERCIO) == lit("4716352")).show()


def main():
    '''Main function.'''
    try:
        #send_queue_mail()
        process()
        #poc_ml()
        # table = read_table(f"tmp/train_features/cluster_id={1}")
        # poc_train2_cluster(table, 1)
        # poc_train_cluster(table, 1)
        # poc_predict_with_existing_model(1, "")
        # poc_dta()
        logger.info('Main process completed.')
    except Exception as e:
        logger.error(f'Main process failed due to the following error: {e}')


def load_transacciones(spark):
    sources = []
    for year in range(2017, 2026 + 1):
        sources.append(
            (f"Year{year}", f"Año{year}")
        )
        if year >= settings.DOUBLE_FROM_YEAR and year != 2026:
            sources.append(
                (f"Year{year}-2", f"Año{year}-2")
            )

    dfs = []
    for database, table in sources:
        df = read_sql_table_w(
            spark,
            f"{database}",
            f"[dbo].[{table}]",
            " CAST(CAST(RUC AS DECIMAL(11,0)) AS VARCHAR(11)) AS RUC, trim(CODCOMERCIO) as CODCOMERCIO, MCC, NOMCOMERCIAL, FECHAOPERACION AS FECHA"
        )
        dfs.append(df)

    for year in range(2017, 2026 + 1):
        df = read_sql_table_w(
            spark,
            "Denegadas",
            f"[dbo].[Den{year}]",
            " null AS RUC, trim(CODCOMERCIO) as CODCOMERCIO, MCC, NOMCOMERCIAL, FECHAOP AS FECHA"
        )
        dfs.append(df)  

    if not dfs:
        return None

    return reduce(lambda a, b: a.unionByName(b), dfs)


def etl(spark):
    #df = read_sql_table(spark, "Prediction", "dbo.CuentasEspeciales", "*" )
    # df.write \
    #     .mode("overwrite") \
    #     .parquet("tmp/cuentas_especiales")
    # df.show(1000)
    # df= (
    #     spark.read
    #     .parquet("tmp/cuentas_especiales")
    # )
    df_tx = load_transacciones(spark)

    df_tx = df_tx.withColumn("FECHA", to_date(col("FECHA")))
    df_tx = df_tx.withColumn("CODCOMERCIO", col("CODCOMERCIO").cast("string"))
    df_tx = df_tx.withColumn("MCC", col("MCC").cast("int"))
    df_tx = df_tx.withColumn("NOMCOMERCIAL", coalesce(col("NOMCOMERCIAL"), lit("SIN NOMBRE COMERCIAL")))

    df_tx = df_tx.orderBy(col("FECHA").desc())

    w = Window.partitionBy("CODCOMERCIO").orderBy(
        col("RUC").isNull().cast("int"),   # 0 = tiene dato, 1 = es NULL
        col("MCC").isNull().cast("int"),
        col("NOMCOMERCIAL").isNull().cast("int")
    )

    df_limpio = (
        df_tx
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    #df_limpio.show(10)
    #print(df_limpio.count())
    df_limpio.write \
        .mode("overwrite") \
        .parquet("tmp/comercios")
    

def etl2(spark):
    df_ce = read_sql_table(
        spark,
        f"DBPrediction",
        f"[dbo].[CuentasEspeciales]",
        "*"
    )
    df_co = (
        spark.read
        .parquet("tmp/comercios")
    )

    df_resultado = (
        df_co.alias("c")
        .join(df_ce.alias("ce"), "RUC", "left")
        .select(
            "c.RUC",
            "c.CODCOMERCIO",
            "c.MCC",
            col("ce.Id").alias("ID_CUENTA_ESPECIAL"),
            #"ce.Id",
            "c.NOMCOMERCIAL"
        )
    )

    df_resultado.write \
        .format("jdbc") \
        .option("url", f"{settings.JDBC_URL};databaseName=DBPrediction") \
        .option("encrypt", "true") \
        .option("trustServerCertificate", "true") \
        .option("dbtable", "[dbo].[_Comercios]") \
        .option("user", settings.JDBC_USER) \
        .option("password", settings.JDBC_PASSWORD) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("batchsize", 10000) \
        .mode("overwrite") \
        .save()


if __name__ == "__main__":
    logger.info('Starts main process.')
    main()
