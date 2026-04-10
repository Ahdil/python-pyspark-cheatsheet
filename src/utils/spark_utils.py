from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def get_spark_session(app_name="SparkSession", master="local[*]") -> SparkSession:
    try:
        jdbc_jar_path = "C:/spark_libs/mssql-jdbc-12.6.1.jre11.jar"

        spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.local.dir", "C:/spark_temp") \
            .config("spark.driver.extraClassPath", jdbc_jar_path) \
            .config("spark.executor.extraClassPath", jdbc_jar_path) \
        .getOrCreate()
        log4j_logger = spark._jvm.org.apache.log4j
        log4j_logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(log4j_logger.Level.OFF)
        logger.info(f"Session Spark '{app_name}' successfully created.")
        return spark
    except Exception as e:
        logger.error(f"Spark initialization error : {e}")
    raise
