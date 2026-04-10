import logging
from pyspark.sql import SparkSession
from pathlib import Path
import pyspark.sql.functions as F
from data.data_generation import create_cad_df_open_close_price

logger = logging.getLogger(__name__)

PATH = Path(__file__).parents[3] / "data"


def simple_arith_func(spark: SparkSession) -> None:
    df = spark.read.parquet(str(PATH / "todos_extended.parquet"))
    df = df.select("userId", "username", "category", "salary", "bonus", "tax").distinct()
    df.withColumn("sal+bonus", F.col("salary") + F.col("bonus")) \
        .withColumn("sal-tax", F.col("salary") - F.col("tax")) \
        .withColumn("benefits", (F.col("salary") * F.lit(0.12)).cast("integer")) \
        .withColumn("average", (F.col("salary") / F.lit(10)).cast("integer")) \
        .withColumn("average", F.col("salary") + 2) \
        .show()


def complex_arith_func(spark: SparkSession) -> None:
    df = create_cad_df_open_close_price(spark)
    df.withColumn('per_value', F.col('diff') / F.col('open') * 100) \
        .withColumn('abs_value', F.abs(F.col('diff'))) \
        .withColumn('per_round_value', F.round(F.col('per_value'), 4)) \
        .withColumn('per_floor_value', F.floor(F.col('per_value')*F.lit(100))) \
        .withColumn('per_ceil_value', F.ceil(F.col('per_value')*F.lit(100))) \
        .withColumn('ceil_value_exp', F.exp(F.col('per_ceil_value'))) \
        .withColumn('log_ceil_value_exp', F.log(F.col('ceil_value_exp'))) \
        .withColumn('power_ceil_value', F.power(F.col('per_ceil_value'), 2)) \
        .withColumn('square_ceil_value', F.sqrt(F.col('power_ceil_value'))) \
        .show()
