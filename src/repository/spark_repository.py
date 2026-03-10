from pyspark.sql import SparkSession
from config.settings import settings

SQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

def create_spark():
    spark =  (
        SparkSession.builder
        .appName("PrediccionesApp")
        .master("local[4]") \
        .config("spark.executor.memory", "12g") \
        .config("spark.executor.cores", "8") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.jars",
                "libs/mssql-jdbc-12.10.2.jre11.jar")
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=libs")
        .getOrCreate()
    )
    #spark.sparkContext.setLogLevel(settings.LOG_LEVEL)
    return spark


def read_sql_table(spark, database, table, fields, join = None, where=None):
    query = f"""
        (SELECT {fields}
         FROM {table}
         {join if join else ""}
         {f"WHERE {where}" if where else ""}
        ) AS t
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", f"{settings.JDBC_URL};databaseName={database}")
        .option("encrypt", "true")
        .option("trustServerCertificate", "true")
        .option("dbtable", query)
        .option("user", settings.JDBC_USER)
        .option("password", settings.JDBC_PASSWORD)
        .option("driver", SQL_DRIVER)
        .option("fetchsize", 100000)
        .load()
    )


def write_sql_table(df_resultado, database, table, mode):
    df_resultado.write \
        .format("jdbc") \
        .option("url", f"{settings.JDBC_URL};databaseName={database}") \
        .option("encrypt", "true") \
        .option("trustServerCertificate", "true") \
        .option("dbtable", table) \
        .option("user", settings.JDBC_USER) \
        .option("password", settings.JDBC_PASSWORD) \
        .option("driver", SQL_DRIVER) \
        .option("batchsize", 10000) \
        .mode(mode) \
        .save()
    

def read_sql_table_w(spark, database, table, fields, join = None, where=None):
    query = f"""
        (SELECT {fields}
         FROM {table}
         {join if join else ""}
         {f"WHERE {where}" if where else ""}
        ) AS t
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", f"{settings.JDBC_URL};databaseName={database};integratedSecurity=true")
        .option("encrypt", "true")
        .option("trustServerCertificate", "true")
        .option("dbtable", query)
        .option("driver", SQL_DRIVER)
        .option("fetchsize", 100000)
        .load()
    )


# Do not run this file directly.
if __name__ == "__main__":
    raise RuntimeError("This file is not meant to be executed directly.")
