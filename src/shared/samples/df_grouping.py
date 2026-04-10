from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging



logger = logging.getLogger(__name__)
PATH = Path(__file__).parents[3] / "data"


def basic_grouping(spark: SparkSession) -> None:
    df = spark.read.parquet(str(PATH / "todos_extended.parquet"))
    logger.info(f"Total rows in table: {df.count()}")
    logger.info(f"Total users in table:{df.select('username').distinct().count()}")

    df.select(F.countDistinct("username")).show()
    df.select('username', 'salary').distinct().select(F.sum("salary").alias('Sum of salaries')).show()
    df.select(F.min("salary").alias("min"), F.max("salary").alias('max')).show()


def aggregate_grouping(spark: SparkSession) -> None:
    schema = (
        "PassengerID INT, Survived INT, Pclass INT, Name STRING, Sex STRING, Age INT, SibSp INT, Parch INT, "
        "Ticket STRING, Fare DOUBLE, Cabin STRING, Embarked STRING")
    df = spark.read.schema(schema).option("header", True).csv(str(PATH / "titanic.csv"))
    df.groupBy("Pclass", "Sex").sum("survived").orderBy("Pclass", "Sex").show()
    df.groupBy("Pclass").agg(F.count("PassengerID").alias("PassengerCount"),
                             F.format_number(F.avg("Fare"), 2).alias("Average_Fare"),
                             F.sum("Survived").alias("Sum_Survived"),
                             ).orderBy("Pclass").show()
    df.groupBy("Pclass").agg(F.count("PassengerID").alias("PassengerCount"),
                             F.format_number(F.avg("Fare"), 2).alias("Average_Fare")
                             ).filter("Average_Fare > 20.00").orderBy("Pclass").show()
