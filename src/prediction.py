from repository.spark_repository import create_spark, read_sql_table, write_sql_table
from config.logging_config import logger
from config.settings import settings
from domain.schemas import MlTrainSchema, ComerciosSchema, CEspecialesSchema, PrediccionesSchema, DenegadasTable, DenegadasSchema
from config.constants import (
    CURRENT_DEN_PATH, PERU_HOLIDAYS, MODEL_DEN_PATH, TABLE_PREDICCIONES_NAME,
    DB_PREDICTION_NAME, TABLE_COMERCIOS_NAME, TABLE_CESPECIALES_NAME,
    AGG_DEN_PATH,
)
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, dayofweek, dayofmonth, weekofyear, year, month, when, 
    date_sub, day, lag, avg, lit, stddev, concat, trim, to_date,
    count, explode, sequence, expr, min
)
from pyspark.sql.types import (
    StructType, StructField, DecimalType, BooleanType,
    IntegerType, DateType, StringType, LongType
)
from azure.storage.queue import QueueClient, TextBase64EncodePolicy
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import lightgbm as lgb
import numpy as np
import json
import os
import shutil


def load_data(spark):
    if os.path.exists(AGG_DEN_PATH):
        shutil.rmtree(AGG_DEN_PATH)

    execution_date = datetime.strptime(settings.EXECUTION_DAY,"%Y-%m-%d")
    start_date = execution_date - timedelta(days=settings.LAG_EXECUTION)

    sources = [
        (
            f"Den{year}",                     # Denegadas Tabla
            f"Year{year}",                    # Transacciones BD
            f"Año{year}",                     # Transacciones Tabla
            year >= settings.DOUBLE_FROM_YEAR # Es doble Transaccion DB Tabla
        )
        for year in range(start_date.year, execution_date.year + 1 )
    ]
   
    for den_tb, tx_db, tx_tb, is_double in sources:
        logger.info(f"Reading : {den_tb}, {tx_db}, {tx_tb}, {is_double}")

        logger.info(f"Loading denied... Table: {den_tb}")
        df_den = read_sql_table(
            spark,
            "Denegadas",
            f"[dbo].[{den_tb}]",
            f"{DenegadasTable.MCC}, {DenegadasTable.CODCOMERCIO}, {DenegadasTable.FECHAOP}"
        )
        logger.info(f"Total denied: {df_den.count()}")

        agregate_data(df_den)


def agregate_data(df_den):
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
    build_ml(df_den, CURRENT_DEN_PATH)
    logger.info("End ml denied")


def build_ml(df, current_path):

    # ✅ Validar que exista al menos un registro exactamente en EXECUTION_DAY
    if not df.filter(
        col(MlTrainSchema.FECHA) == date_sub(to_date(lit(settings.EXECUTION_DAY)), 1)
    ).take(1):
        logger.error(f"No existen registros para el día anterior a {settings.EXECUTION_DAY}")
        raise Exception(f"No existen registros para el día anterior a {settings.EXECUTION_DAY}")
    

    logger.info("Add ml columns")

    df = (
        df
        .withColumn(MlTrainSchema.CODCOMERCIO, col(MlTrainSchema.CODCOMERCIO).cast('long'))
        .filter(
            col(MlTrainSchema.FECHA) <= to_date(lit(settings.EXECUTION_DAY))
        )
    )

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
    
    df_last_x = (
        df_dense
        .filter(
            col(MlTrainSchema.FECHA) >= date_sub(lit(settings.EXECUTION_DAY), settings.LAG_EXECUTION)
        )
    )

    logger.info("Save minimal lag parquet") 
    df_last_x.write \
        .mode("overwrite") \
        .partitionBy(MlTrainSchema.MCC) \
        .parquet(current_path)


def skip_model(model_path):
    return (
        MlTrainSchema.MCC not in model_path 
        or model_path == "model_MCC=-1.txt"
        or model_path == "model_MCC=0.txt"
    )


def load_lag_data(spark, date, data_path, model_path, df_comercios):
    mcc = int(
        model_path.replace(f"model_{MlTrainSchema.MCC}=", "").replace(".txt", "")
    )
    path = f"{data_path}/{MlTrainSchema.MCC}={mcc}"
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"data not exists for MCC={mcc} on path: {path}"
        )
    df_data = (
        spark.read
        .parquet(path)
        .filter(col(MlTrainSchema.FECHA) < date)
        .withColumn(MlTrainSchema.MCC, lit(mcc))
    )
    df_comercios = (
        df_comercios.filter(col(MlTrainSchema.MCC) == mcc)
    )
    return df_data, df_comercios


def build_prediction_ml(date, df_data, df_comercios):
    df_last = (
        df_data
        .filter(col(MlTrainSchema.FECHA) == date_sub(lit(date), 1))
        .select(
            MlTrainSchema.CODCOMERCIO,
            lit(date).alias(MlTrainSchema.FECHA),
            lit(0).alias(MlTrainSchema.TOTAL)
        )
    )

    df_predict = (
        df_last
        .withColumn(MlTrainSchema.DOW, dayofweek(lit(date)))
        .withColumn(MlTrainSchema.WEEK, weekofyear(lit(date)))
        .withColumn(MlTrainSchema.DAY, day(lit(date)))
        .withColumn(MlTrainSchema.MONTH, month(lit(date)))
        .withColumn(MlTrainSchema.YEAR, year(lit(date)))
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

    df_predict = df_data.unionByName(df_predict, allowMissingColumns=True)

    w_com = Window.partitionBy(
        MlTrainSchema.CODCOMERCIO
    ).orderBy(MlTrainSchema.FECHA)
    w_co_7 = w_com.rowsBetween(-7, -1)
    w_co_14 = w_com.rowsBetween(-14, -1)

    df_predict = ( 
        df_predict.withColumn(MlTrainSchema.DEN_LAG_1, lag(MlTrainSchema.TOTAL, 1).over(w_com))
        .withColumn(MlTrainSchema.DEN_LAG_2, lag(MlTrainSchema.TOTAL, 2).over(w_com))
        .withColumn(MlTrainSchema.DEN_LAG_7, lag(MlTrainSchema.TOTAL, 7).over(w_com))
        .withColumn(MlTrainSchema.DEN_LAG_14, lag(MlTrainSchema.TOTAL, 14).over(w_com))
        .withColumn(MlTrainSchema.DEN_LAG_30, lag(MlTrainSchema.TOTAL, 30).over(w_com))
        .withColumn(
            MlTrainSchema.DEN_MA_7,
            avg(MlTrainSchema.TOTAL).over(w_co_7)
        )
        .withColumn(
            MlTrainSchema.DEN_MA_14,
            avg(MlTrainSchema.TOTAL).over(w_co_14)
        )
        .withColumn(MlTrainSchema.DEN_STD_7, stddev(MlTrainSchema.TOTAL).over(w_co_7))
        .withColumn(MlTrainSchema.DIFF_WEEK, col(MlTrainSchema.DEN_LAG_1) - col(MlTrainSchema.DEN_LAG_7))
        .fillna(0)
        .orderBy(MlTrainSchema.FECHA)
    )    

    df_predict = (
        df_predict
        .filter(col(MlTrainSchema.FECHA) == date)
    )

    df_predict = (
        df_predict
        .join(df_comercios.alias("C"), PrediccionesSchema.CODCOMERCIO, "left")
        .select(
            f"C.{PrediccionesSchema.RUC}",
            #f"C.{PrediccionesSchema.CODCOMERCIO}",
            f"C.{PrediccionesSchema.MCC}",
            col(ComerciosSchema.ID).alias(PrediccionesSchema.ID_COMERCIO),
            MlTrainSchema.FECHA,
            * MlTrainSchema.FEATURES
        )
    )

    return df_predict


def prepare_data(spark):
    execution_date = datetime.strptime(settings.EXECUTION_DAY,"%Y-%m-%d").date()
    logger.info(f"Execution day for : {execution_date}")

    logger.info("Load data for comercios")
    df_comercios = read_sql_table(
        spark,
        DB_PREDICTION_NAME,
        TABLE_COMERCIOS_NAME,
        f"C.{ComerciosSchema.ID}, C.{ComerciosSchema.RUC}, C.{ComerciosSchema.CODCOMERCIO}, C.{ComerciosSchema.MCC}, C.{ComerciosSchema.ID_CUENTA_ESPECIAL}, CS.{CEspecialesSchema.CORREO_NOTI}",
        f"LEFT JOIN {TABLE_CESPECIALES_NAME} ON C.{ComerciosSchema.ID_CUENTA_ESPECIAL} = CS.{CEspecialesSchema.ID}"
    )
    
    for model_path in os.listdir(MODEL_DEN_PATH):
        logger.info("Reading models in: " + model_path)
        if skip_model(model_path):
            continue
        logger.info(f"Prepare denied model for {model_path}")
        try:
            df_data, df_com_mcc = load_lag_data(
                spark, 
                execution_date,
                CURRENT_DEN_PATH, 
                model_path,
                df_comercios
            )
            df_predict = build_prediction_ml(
                execution_date,
                df_data,
                df_com_mcc
            )
            results = predict_next_day(
                df_predict,
                f"{MODEL_DEN_PATH}/{model_path}"
            )
            logger.info("Save results of predictions ...")
            save_predictions(spark, results)
        except Exception as e:
            logger.warning(f"the model {model_path} can not be procesed")
            logger.warning(str(e))


def predict_next_day(df_predict, model_path):
    model = lgb.Booster(model_file=model_path)
    logger.info("Model loaded...")

    rows = df_predict.collect()
    logger.info("Generated predition list")

    results = []

    for row in rows:
        X = np.array([row[f] for f in MlTrainSchema.FEATURES]).reshape(1, -1)

        pred = model.predict(X)[0]

        pred_dec = Decimal(str(pred)).quantize(
            Decimal("0.0000000000"), rounding=ROUND_HALF_UP
        )
        den_ma_7_dec = Decimal(str( row[MlTrainSchema.DEN_MA_7]))

        UMBRAL_MINIMO = Decimal("5")
        notifica = (
            pred_dec >= den_ma_7_dec
            and den_ma_7_dec != Decimal("0.00")
            and pred_dec >= UMBRAL_MINIMO
        )

        if(notifica):
            send_queue_mail(
                row[MlTrainSchema.CODCOMERCIO],
                round(pred, 2)
            )

        results.append({
            PrediccionesSchema.RUC: row[PrediccionesSchema.RUC],
            PrediccionesSchema.CODCOMERCIO: row[MlTrainSchema.CODCOMERCIO],
            PrediccionesSchema.MCC: row[MlTrainSchema.MCC],
            PrediccionesSchema.ID_COMERCIO: row[PrediccionesSchema.ID_COMERCIO],
            PrediccionesSchema.FECHA: row[MlTrainSchema.FECHA],
            PrediccionesSchema.PREDICCION: pred_dec,
            PrediccionesSchema.NOTIFICA: notifica
        })

    return results


def save_predictions(spark, results):
    predicciones_schema = StructType([
        StructField(PrediccionesSchema.RUC, StringType(), True),
        StructField(PrediccionesSchema.CODCOMERCIO, StringType(), True),
        StructField(PrediccionesSchema.MCC, IntegerType(), True),
        StructField(PrediccionesSchema.ID_COMERCIO, IntegerType(), True),
        StructField(PrediccionesSchema.FECHA, DateType(), True),
        StructField(PrediccionesSchema.PREDICCION, DecimalType(18, 10), False),
        StructField(PrediccionesSchema.NOTIFICA, BooleanType(), False)
    ])
    df_results = spark.createDataFrame(
        results,
        schema=predicciones_schema
    )

    write_sql_table(df_results, DB_PREDICTION_NAME, TABLE_PREDICCIONES_NAME, "append")


def send_queue_mail(code, prediction):
    logger.info("Send notify by email")
    queue_client = QueueClient.from_connection_string(
        conn_str=settings.QUEUE_CN,
        queue_name=settings.QUEUE_NAME,
        message_encode_policy=TextBase64EncodePolicy()
    )
    mensaje = {
        "notification_id": 0,
        "to": [
            "dev.expressnet@devmatte.com", "pcherre@expressnet.com.pe"
        ],
        "cc": [],
        "template_id": "10014",
        "template_data": {
            "code":code,
            "prediction": prediction
        },
        "attachments": []
    }

    queue_client.send_message(json.dumps(mensaje))
    

def process():
    '''Processing...'''
    logger.info("Creating Spark session...")
    spark = create_spark()
    logger.info("Spark session created...")

    logger.info("Generate lag data for predictions...")
    load_data(spark)
    logger.info("Lag data generated for predictions...")

    logger.info("Build lag data for predictions...")
    build_data(spark)
    logger.info("Lag data builed for predictions...")

    logger.info("Load data for predictions...")
    prepare_data(spark)

    spark.stop()


def main():
    '''Prediction function.'''
    try:
        start_time = datetime.now()
        logger.info(f"Starting processing at {start_time:%Y-%m-%d %H:%M:%S}")

        process()

        end_time = datetime.now()
        logger.info(f"Finished processing at {end_time:%Y-%m-%d %H:%M:%S}")
        logger.info(f"Total duration: {end_time - start_time}")

        logger.info('Prediction process completed.')
    except Exception as e:
        logger.error(f'Prediction process failed due to the following error: {e}')


if __name__ == "__main__":
    logger.info('Starts prediction process.')
    main()
