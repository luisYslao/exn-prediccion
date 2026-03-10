from datetime import datetime, date
from config.logging_config import logger
from pyspark.sql import SparkSession
from config.settings import settings
from pyspark.sql.functions import (
    col, count, to_date, dayofweek, weekofyear, year, 
    month, when, coalesce, explode, sequence, day,
    lag, avg, lit, to_date, expr, min, trim, nullif
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
from datetime import timedelta
from domain.schemas import DenegadasSchema, DenegadasTable, MlTrainSchema, TransaccionesSchema, TransaccionesTable

AGG_DEN_PATH = "tmp/aggregated_features/denied"
AGG_TX_PATH = "tmp/aggregated_features/transactions"
TRAIN_DEN_PATH = "tmp/train_features/denied"
TRAIN_TX_PATH = "tmp/train_features/transactions"
CURRENT_DEN_PATH = "tmp/current_features/denied"
CURRENT_TX_PATH = "tmp/current_features/transactions"

def create_spark():
    return (
        SparkSession.builder
        .appName("PrediccionesApp")
        .master("local[4]") \
        .config("spark.executor.memory", "12g") \
        .config("spark.executor.cores", "8") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "8") \
        # .master("local[6]") \
        # .config("spark.executor.memory", "12g") \
        # .config("spark.driver.memory", "8g") \
        # .config("spark.executor.cores", "6") \
        # .config("spark.sql.shuffle.partitions", "6") \
        # .config("spark.memory.fraction", "0.8") \
        .config("spark.jars",
                "libs/mssql-jdbc-12.10.2.jre11.jar")
        # ONLY FOR INTEGRATED SECURITY
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=libs")
        # END
        .getOrCreate()
    )


def read_sql_table(spark, database, table, fields):
    query = f"""
        (SELECT {fields}
         FROM {table}) AS t
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", f"{settings.JDBC_URL};databaseName={database};integratedSecurity=true")
        .option("encrypt", "true")
        .option("trustServerCertificate", "true")
        .option("dbtable", query)
        #.option("dbtable", table)
        #.option("user", settings.JDBC_USER)
        #.option("password", settings.JDBC_PASSWORD)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("fetchsize", 100000)
        .load()
    )


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

        logger.info(f"Loading transactions... Db: {txDb}, Table: {txTb}")
        df_tx = read_sql_table(
            spark,
            f"{txDb}",
            f"[dbo].[{txTb}]",
            f"{TransaccionesTable.MCC},{TransaccionesTable.CODCOMERCIO}, {TransaccionesTable.FECHAOPERACION}"
        )
        logger.info(f"Total transactions: {df_tx.count()}")

        if IsDouble:
            logger.info(f"Loading transactions 2... Db: {txDb}-2, Table: {txTb}-2")
            df_tx2 = read_sql_table(
                spark,
                f"{txDb}-2",
                f"[dbo].[{txTb}-2]",
                f"{TransaccionesTable.MCC},{TransaccionesTable.CODCOMERCIO}, {TransaccionesTable.FECHAOPERACION}"
            )
            logger.info(f"Total transactions 2: {df_tx2.count()}")
            df_tx = df_tx.unionByName(df_tx2)
            logger.info(f"Total transactions Union: {df_tx.count()}")

        agregate_data(df_den, df_tx)


def agregate_data(df_den, df_tx):
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

    df_tx_agregated = (
        df_tx
        .withColumn(
            TransaccionesTable.CODCOMERCIO,
            trim(col(TransaccionesTable.CODCOMERCIO))
        )
        .filter(col(TransaccionesTable.CODCOMERCIO) != "0")
        .withColumn(
            TransaccionesSchema.FECHA,
            to_date(col(TransaccionesTable.FECHAOPERACION))
        )
        .withColumn(
            TransaccionesTable.MCC,
            when(
                col(TransaccionesTable.MCC).isNull() |
                (trim(col(TransaccionesTable.MCC)) == ""),
                lit("-1")
            ).otherwise(trim(col(TransaccionesTable.MCC))).cast('int')
        )
        .groupBy(
            TransaccionesTable.CODCOMERCIO,
            TransaccionesSchema.FECHA,
            TransaccionesTable.MCC
        )
        .agg(
            count("*").alias(TransaccionesSchema.TOTAL)
        )
    )
    logger.info(f"Total transactions AGG: {df_tx_agregated.count()}")

    df_tx_agregated.write \
        .mode("append") \
        .partitionBy(TransaccionesSchema.MCC) \
        .parquet(AGG_TX_PATH)


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

    # logger.info("Prepare data for transactions")
    # tx_schema = StructType([
    #     StructField(MlTrainSchema.MCC, IntegerType(), False),
    #     StructField(MlTrainSchema.CODCOMERCIO, StringType(), False),
    #     StructField(MlTrainSchema.FECHA, DateType(), False),
    #     StructField(MlTrainSchema.TOTAL, LongType(), False),
    # ])
    # df_tx = (
    #     spark.read
    #     .schema(tx_schema)
    #     .parquet(AGG_TX_PATH)
    # )
    # logger.info(f"total loaded transactions: {df_tx.count()}")
    # build_ml(df_tx, TRAIN_TX_PATH, CURRENT_TX_PATH)
    # logger.info("End ml transactions")


def build_ml(df, train_path):
    logger.info("Add ml columns")

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
    )

    logger.info("Generating lag fields") 
    w_mcc_com = Window.partitionBy(
        MlTrainSchema.MCC,
        MlTrainSchema.CODCOMERCIO
    ).orderBy(MlTrainSchema.FECHA)
    # w_mcc_co_4 = w_mcc_com.rowsBetween(-4, -1)
    w_mcc_co_7 = w_mcc_com.rowsBetween(-7, -1)
    w_mcc_co_14 = w_mcc_com.rowsBetween(-14, -1)
    w_mcc_co_30 = w_mcc_com.rowsBetween(-30, -1)

    df_dense = ( 
        df_dense.withColumn(MlTrainSchema.DEN_LAG_1, lag(MlTrainSchema.TOTAL, 1).over(w_mcc_com))
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
        .withColumn(
            MlTrainSchema.DEN_MA_30,
            avg(MlTrainSchema.TOTAL).over(w_mcc_co_30)
        )
        .fillna(0)
    )    

    logger.info("Save parquet")  
    df_dense.write \
        .mode("overwrite") \
        .partitionBy(MlTrainSchema.MCC) \
        .parquet(train_path)


def prepare_cluster():
    for mcc_path in os.listdir(TRAIN_DEN_PATH):
        if(MlTrainSchema.MCC in mcc_path):
            logger.info(f"Prepare model for {mcc_path}")
            data = pq.read_table(f"{TRAIN_DEN_PATH}/{mcc_path}")
            train_model(data, mcc_path, "denied")
    #for mcc_path in os.listdir(TRAIN_TX_PATH):
    #    print(mcc_path)
    #    if("MCC" in mcc_path):
    #        data = pq.read_table(f"{TRAIN_TX_PATH}/{mcc_path}")
    #        train_model(data, mcc_path, "transactions")


def prepare_cluster_unique():
    data = pq.read_table(f"{TRAIN_DEN_PATH}")
    train_model(data, "unique", "_denied")

    data = pq.read_table(f"{TRAIN_TX_PATH}")
    train_model(data, "unique", "_transactions")


def train_model(data, cluster, type):
    X = np.column_stack([data.column(c).to_numpy() for c in MlTrainSchema.FEATURES])
    y = data.column(MlTrainSchema.TOTAL).to_numpy()

    train_data = lgb.Dataset(X, label=y)

    model = lgb.train(
        #{"objective": "regression", "metric": "rmse"},
        {
            "objective": "poisson",
            "metric": ["rmse", "mae"],
            "learning_rate": 0.05,
            "num_leaves": 31,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "min_data_in_leaf": 20,
            "verbose": -1,
            "poisson_max_delta_step": 0.7
        },
        # {
        #     "objective": "tweedie",
        #     "tweedie_variance_power": 1.2,
        #     "metric": ["mae"],
        #     "learning_rate": 0.05,
        #     "num_leaves": 31,
        #     "min_data_in_leaf": 50,
        #     "feature_fraction": 0.8,
        #     "bagging_fraction": 0.8,
        #     "bagging_freq": 5,
        #     "verbose": -1
        # },
        train_data,
        num_boost_round=200
    )

    output_dir = f"tmp/models/{type}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    model.save_model(f"tmp/models/{type}/model_{cluster}.txt")


def process():
    '''Processing...'''
    start_time = datetime.now()
    logger.info(f"Starting processing at {start_time:%Y-%m-%d %H:%M:%S}")

    logger.info("Creating Spark session...")
    #spark = create_spark()

    # To enable detailed logging, uncomment the following line
    # spark.sparkContext.setLogLevel("DEBUG")

    logger.info("Loading Data")
    #load_data(spark)

    logger.info("Build Data")
    #build_data(spark)

    logger.info("Prepare Cluster")
    prepare_cluster()
    #prepare_cluster_unique()

    predict_next_day()

    end_time = datetime.now()
    logger.info(f"Finished processing at {end_time:%Y-%m-%d %H:%M:%S}")
    logger.info(f"Total duration: {end_time - start_time}")


def poc_predict_with_existing_model(cluster_id, data_path):
    FEATURES = ["total_tx", "dow", "week", "month", "is_weekend", "den_lag_1",
                "den_lag_7", "den_ma_7"]

    model_path = f"tmp/models/model_cluster_{cluster_id}.txt"
    bst = lgb.Booster(model_file=model_path)

    table = read_table(data_path, columns=FEATURES)
    new = np.column_stack([table.column(c).to_numpy() for c in FEATURES])

    # 3. Predecir
    predictions = bst.predict(new)
    return predictions


def predict_next_day():
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


def main():
    '''Main function.'''
    try:
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


if __name__ == "__main__":
    logger.info('Starts main process.')
    main()
