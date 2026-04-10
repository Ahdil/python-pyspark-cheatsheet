from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pathlib import Path
import logging

PATH = Path(__file__).parents[3] / "data"

logger = logging.getLogger(__name__)

def basic_filter(spark: SparkSession) -> None:
    df = spark.read.format("parquet").load(str(PATH / "todos_extended.parquet"))
    df.filter(F.col("salary") > 150000).select("username", "salary").distinct().orderBy("salary").show()
    df.filter(df.salary > 150000).select("username", "salary").distinct().orderBy("salary").show()
    df.filter(df['salary'] > 150000).select("username", "salary").distinct().orderBy("salary").show()

def multiple_conditions_filter(spark: SparkSession) -> None:
    df = spark.read.format("parquet").load(str(PATH / "todos_extended.parquet"))
    df.filter((df.salary < 50000) & df.completed).select("username", "salary", "completed") \
        .distinct().orderBy("salary").show()
    df.where((df.salary < 50000) | df.completed).select("username", "salary", "completed") \
         .distinct().orderBy("salary").show()

def string_filter(spark: SparkSession) -> None:
    df = spark.read.format("parquet").load(str(PATH / "todos_extended.parquet"))
    df.where(F.upper(df.username) == "LUKE").select("username", "salary", F.lit(True) \
        .alias("Selected")).distinct().show()

    df.where(F.upper(F.col("title")).like("%IPSA%")).select("username", "title",).show()
    df.where(F.col("title").contains("ipsam")).select("username", "title", ).show()
    df.where(F.col("title").startswith("ipsam")).select("username", "title", ).show()
    df.where(F.col("title").endswith("quam")).select("username", "title", ).show(truncate=False)
    df.where(F.col("title").rlike("^.*quam$")).select("username", "title", ).show(truncate=False)


def null_filter(spark: SparkSession) -> None:
    df = spark.read.format("parquet").load(str(PATH / "todos_extended.parquet"))
    df.filter(F.col("username").isNull()).select("username", "salary").distinct().show()
    df.filter(~F.col("username").isNull()).select("username", "salary").distinct().show()
    df.filter(F.col("username").isNotNull()).select("username", "salary").distinct().show()

def from_list_filter(spark: SparkSession) -> None:
    df = spark.read.format("parquet").load(str(PATH / "todos_extended.parquet"))
    names = ['FRANK', 'KIM']
    df.where(F.upper(F.col('username')).isin(names)).select('username', "salary").distinct().show()
    df.where(~F.upper(F.col('username')).isin(names)).select('username', "salary").distinct().show()

def data_cleansing_filter(spark: SparkSession) -> None:
    df = spark.read.format("parquet").load(str(PATH / "todos_extended.parquet"))
    df.dropDuplicates().select("username", "salary").show()
    df.dropDuplicates(["username"]).select("username", "salary").show()

    df.dropna().select("username", "salary").show()
    df.dropna(subset=['userid', 'id']).select("username", "salary", "userid", "id").show()

    df.fillna({'currency': 'EUR'}).select('userid', 'currency').distinct().orderBy('userid').show()
    df.dropna(subset=['currency']).select('userid', 'currency').distinct().orderBy('userid').show()


