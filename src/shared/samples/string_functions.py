import logging
from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
PATH = Path(__file__).parents[3] / "data"


def get_local_df(spark: SparkSession, all_fields: bool = False) -> DataFrame:
    df = spark.read.option("header", True).csv(str(PATH / "titanic.csv"))
    df = df.withColumn('LastName', F.split('Name', ",")[0]) \
        .withColumn('Title_LastName', F.split('Name', ",")[1]) \
        .withColumn('Title', F.split('Title_LastName', "\.")[0]) \
        .withColumn('FirstName', F.split('Title_LastName', "\.")[1])
    if all_fields:
        return df
    return df.select('Sex', 'Cabin', 'Embarked', 'FirstName', 'Title', 'LastName')


def basic_string_func(spark: SparkSession) -> None:
    df = get_local_df(spark)

    df.withColumn("Conc_Embarked_Cabin", F.concat('Embarked', 'Cabin')) \
        .select('Embarked', 'Cabin', 'Conc_Embarked_Cabin') \
        .show(5, truncate=False)

    df.withColumn("Conc_Embarked_Cabin", F.concat_ws(' ', 'Embarked', 'Cabin')) \
        .select('Embarked', 'Cabin', 'Conc_Embarked_Cabin') \
        .show(5, truncate=False)

    df.withColumn("Conc_Embarked_Cabin", F.concat_ws(',', 'Embarked', 'Cabin')) \
        .select('Embarked', 'Cabin', 'Conc_Embarked_Cabin') \
        .show(5, truncate=False)

    df.withColumn("Name", F.concat_ws('', F.lit('Name:'), 'FirstName', F.lit(' '), 'LastName')) \
        .select('Name') \
        .show(5, truncate=False)

    df.withColumn("contains_ja", F.col("FirstName").contains(F.lit("Ja"))) \
        .withColumn("contains_ja2", F.contains(df["FirstName"], F.lit("Ja"))) \
        .select('FirstName', 'contains_ja', 'contains_ja2') \
        .show(5, truncate=False)

    df.withColumn("start_ja", F.col("FirstName").startswith(F.lit(" Ja"))) \
        .withColumn("start_ja2", F.startswith(df["FirstName"], F.lit(" Jacques"))) \
        .select('FirstName', 'start_ja', 'start_ja2') \
        .show(5, truncate=False)

    df.withColumn("end_ris", F.col("FirstName").endswith(F.lit("ris"))) \
        .withColumn("end_ris2", F.endswith(df["FirstName"], F.lit("ris"))) \
        .select('FirstName', 'end_ris', 'end_ris2') \
        .show(5, truncate=False)

    df.withColumn("InitCap", F.initcap(df["Sex"])) \
        .select('InitCap') \
        .show(5, truncate=False)

    df.withColumn("FIRSNANE", F.upper(df["FirstName"])) \
        .select('FIRSNANE') \
        .show(5, truncate=False)

    df.withColumn("FirstName", F.lower(df["FirstName"])) \
        .select('FirstName') \
        .show(5, truncate=False)

    df.withColumn("FirstName_lenght", F.length(df["FirstName"])) \
        .select('FirstName', "FirstName_lenght") \
        .show(5, truncate=False)


def trim_pad_functions(spark: SparkSession) -> None:
    df = get_local_df(spark)

    df.withColumn("LastName2", F.rpad(df["LastName"], 30, " ")) \
        .withColumn("LastName3", F.lpad(F.col("LastName2"), 35, " ")) \
        .withColumn("LastName4", F.ltrim(F.col("LastName3"))) \
        .withColumn("LastName5", F.rtrim(F.col("LastName3"))) \
        .withColumn("LastName6", F.trim(F.col("LastName3"))) \
        .select('LastName', 'LastName3', 'LastName4', 'LastName5', 'LastName6') \
        .show(5, truncate=False)

    df.withColumn("Embarked", F.rpad(df["Embarked"], 7, "0")) \
        .show(5, truncate=False)


def extracting_functions(spark: SparkSession) -> None:
    df = get_local_df(spark)
    df.withColumn("FirstName1", F.substring(df["FirstName"], 1, 8)) \
        .withColumn("FirstName2", F.substring(df["FirstName"], -8, 8)) \
        .show(5, truncate=False)


def converting_functions(spark: SparkSession) -> None:
    df = get_local_df(spark, True)

    df = df.withColumn("Fare", F.col("Fare").cast("double")) \
        .withColumn("Age", F.col("Age").cast("int")) \
        .withColumn("Survived", F.col("Survived").cast("boolean")) \
        .withColumn("SSurvived", F.col('Survived').cast("string")) \
        .select('Firstname', 'LastName', 'Fare', 'Age', 'Survived', 'SSurvived')
    df.show(5, truncate=False)
    df.printSchema()
