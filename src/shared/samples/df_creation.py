from data.data_generation import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
import logging

logger = logging.getLogger(__name__)

def create_df_with_default_schema(spark:SparkSession) -> None:
    data = [item[:2] for item in generate_data_as_list_of_rows()]
    columns = ["Nom", "Age"]
    df = spark.createDataFrame(data, schema=columns)
    print(f"-----Schema with {columns} -------")
    df.printSchema()
    df.show()

def create_df_with_explicit_struct_type_schema(spark:SparkSession) -> None:

    data = [item[:2] for item in generate_data_as_list_of_rows()]
    schema = StructType([
        StructField("Nom", StringType(), False),
        StructField("Age", IntegerType(), False),
        ])
    df = spark.createDataFrame(data, schema=schema)
    print(f"-----Schema with {schema} -------")
    df.printSchema()
    df.show()

def create_df_with_explicit_struct_type_schema_dates(spark:SparkSession):
    data = [item[:4] for item in generate_data_as_list_of_rows()]
    schema = StructType([
       StructField("Nom", StringType(), False),
       StructField("Age", IntegerType(), False),
       StructField("Birth", DateType(), False),
       StructField("logged", TimestampType(), False),
    ])
    df = spark.createDataFrame(data, schema=schema)
    return df

def create_df_with_explicit_sql_schema(spark:SparkSession) -> None:
    data = generate_data_as_list_of_rows()
    schema = "Nom STRING, Age INT, Birth DATE, logged TIMESTAMP, Active BOOLEAN"
    df = spark.createDataFrame(data, schema=schema)
    print(f"-----Schema with {schema} -------")
    df.printSchema()
    df.show()

def create_df_with_inferred_dictionary(spark:SparkSession) -> None:
    data = generate_data_as_list_of_dictionaries()
    df = spark.createDataFrame(data)
    print(f"-----Schema with dictionary -------")
    df.printSchema()
    df.show()

