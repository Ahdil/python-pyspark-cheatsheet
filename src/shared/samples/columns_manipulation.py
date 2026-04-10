from pyspark.sql import SparkSession
from pathlib import Path
import pyspark.sql.functions as F
import logging

from shared.samples.file_writing import rebuild_unique_file, TEMP_PATH

logger = logging.getLogger(__name__)

PATH = Path(__file__).parents[3] / "data"


def column_selector(spark: SparkSession, in_sql: bool = True) -> None:
    columns = ["Nom", "Birth"]
    df = spark.read.format("parquet").load(str(PATH / "users.parquet"))
    if in_sql:
        df.createOrReplaceTempView("users")
        spark.sql(f"select {', '.join(columns)} from users").show()
        spark.sql(f"select {', '.join(columns)} from {{users}}", users=df).show()
    else:
        df.select(*columns).show()


def column_rename(spark: SparkSession) -> None:
    df = spark.read.format("parquet").load(str(PATH / "users.parquet"))
    df.withColumnRenamed("Birth", "Birthday").show()
    df.select(F.col("Nom").alias("Name"), F.col("Birth").alias("Birthday"), F.col("Age"), F.col("logged")).show()


def add_columns(spark: SparkSession) -> None:
    df = spark.read.format("parquet").load(str(PATH / "todos.parquet"))
    df = spark.sql("""Select
              id
              , title
              , completed
              , userid
              , case userid
                    when 1 then "Eloise"
                    when 2 then "Frank"
                    when 3 then "Henri"
                    when 4 then "Jean"
                    when 5 then "Kim"
                    when 6 then "Luke"
                    when 7 then "Alice"
                    when 8 then "Bob"
                    when 9 then "Charlie"
                    when 10 then "David"
                end as username
              , case userid
                    when 1 then 84000
                    when 2 then 62000
                    when 3 then 74000
                    when 4 then 357000
                    when 5 then 85000
                    when 6 then 25000
                    when 7 then 140000
                    when 8 then 58000
                    when 9 then 78000
                    when 10 then 480000
                end as salary
              from {todos}""", todos=df)
    # noinspection PyTypeChecker
    df = df.withColumns({
        "category": F.when(F.col("salary") > 150000, "high") \
            .when(F.col("salary") > 75000, "medium") \
            .otherwise("low"),
        "bonus": F.col("salary") * 0.1,
        "tax": F.expr("salary * 0.2"),
        "active": F.lit(True),
        "currency": F.when(F.col('userid') <= 3, 'USD') \
            .when(F.col('userid').isin(6, 7), 'CAD') \
            .when(F.col('userid') == 9, 'GBP')

    })
    df = df.withColumn("id", F.col("id").cast("long").alias("id", metadata={"nullable": False}))
    df = df.withColumn("bonus", F.col("bonus").cast("integer"))
    df = df.withColumn("tax", F.col("tax").cast("integer"))
    df.select("username", "bonus", "tax", "salary", "category", "active").distinct().orderBy("salary").show()
    df.write.option("overwrite", True).parquet(str(TEMP_PATH))
    rebuild_unique_file(str(PATH / "todos_extended.parquet"), "parquet")


def drop_columns(spark: SparkSession) -> None:
    df = spark.read.format("parquet").load(str(PATH / "todos_extended.parquet"))
    df.drop("active").show()
    df.drop("bonus", "tax").show()
