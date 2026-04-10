from pyspark.sql import SparkSession


from data.data_generation import get_json_todos, rebuild_unique_file
from shared.samples.df_creation import create_df_with_explicit_struct_type_schema_dates
from pathlib import Path
import logging

logger = logging.getLogger(__name__)
TEMP_PATH = Path("/") / "python_temp"

def basic_csv_writer(spark: SparkSession, path: str) -> None:
    df = create_df_with_explicit_struct_type_schema_dates(spark)
    df.coalesce(1).write.mode("overwrite").option("header", "True").csv(str(TEMP_PATH))
    rebuild_unique_file(path)

def with_options_csv_writer(spark: SparkSession, path: str) -> None:
    df = create_df_with_explicit_struct_type_schema_dates(spark)
    df.coalesce(1).write.mode("overwrite") \
        .option("dateFormat", "yyyy-MM-dd") \
        .option("delimiter", ";" ) \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("header", "True") \
        .csv(str(TEMP_PATH))
    rebuild_unique_file(path)

def basic_json_writer(spark: SparkSession, path: str) -> None:
    df = create_df_with_explicit_struct_type_schema_dates(spark)
    df.coalesce(1).write.mode("overwrite").json(str(TEMP_PATH))
    rebuild_unique_file(path, 'json')

def basic_parquet_writer(spark: SparkSession, path: str) -> None:
    df = create_df_with_explicit_struct_type_schema_dates(spark)
    df.coalesce(1).write.option("compression", "none").mode("overwrite").parquet(str(TEMP_PATH))
    rebuild_unique_file(path, 'parquet')
    df = spark.read.parquet(path)
    df.show()

def write_json_parquet(spark: SparkSession, path: str) -> None:
    json_path = get_json_todos()
    df = spark.read.json(str(json_path), multiLine=True )
    df.coalesce(1).write.mode("overwrite").parquet(str(TEMP_PATH))
    rebuild_unique_file(path, 'parquet')
    df = spark.read.parquet(path)
    df.show()

