from pathlib import Path
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def get_csv_file_path() -> str:
    current_path = Path(__file__).resolve()
    root_path = current_path.parents[2]
    csv_file_path = root_path.parent / "data" / "titanic.csv"
    return str(csv_file_path)

def read_csv_file_inferred(spark: SparkSession) -> None:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(get_csv_file_path())
    df.printSchema()
    df.show()

def read_csv_file(spark: SparkSession) -> None:
    df = spark.read.format("csv").load(get_csv_file_path(), inferSchema=True, header=True)
    df.printSchema()
    df.show()

def read_csv_file_with_type(spark: SparkSession) -> None:
    from pyspark.sql.types import StringType, IntegerType,StructType, StructField, DoubleType
    schema = StructType([
        StructField("PassengerId", IntegerType(), False),
        StructField("Survived", IntegerType(), True),
        StructField("Pclass", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("Sex", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("SibSp", IntegerType(), True),
        StructField("Parch", IntegerType(), True),
        StructField("Ticket", StringType(), True),
        StructField("Fare", DoubleType(), True),
        StructField("Cabin", StringType(), True),
        StructField("Embarked", StringType(), True),
    ])
    df = spark.read.format("csv").schema(schema).option("header", True).load(get_csv_file_path() )
    df.printSchema()
    df.show()

def read_json_file(spark: SparkSession) -> None:
    from data.data_generation import get_json_todos
    df = spark.read.option("multiline", "true").json(str(get_json_todos()))
    df.printSchema()
    df.show()

def read_json_file_with_explicit_schema(spark: SparkSession) -> None:
    from data.data_generation import get_json_todos
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), True),
        StructField("userId", IntegerType(), True),
        StructField("completed", BooleanType(), True),
    ])
    df = spark.read.format("json").option("multiline", "true").schema(schema).load(str(get_json_todos()))
    df.printSchema()
    df.show()


