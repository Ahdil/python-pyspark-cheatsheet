import logging
from pyspark.sql import SparkSession, functions as F, types as T

from data.data_generation import create_cad_json_dataframe, create_xau_json_dataframe, create_cad_df_open_close_price

logger = logging.getLogger(__name__)


def single_field_manipulation(spark: SparkSession) -> None:
    df = create_cad_json_dataframe(spark)
    df.withColumn("April_07_2026", F.get_json_object("Time Series FX (Daily)", "$.2026-04-07")) \
        .select("April_07_2026").show(truncate=False)
    df.withColumn("April_07_2026_close",
                  F.get_json_object("Time Series FX (Daily)", "$['2026-04-07']['4. close']")) \
        .select("April_07_2026_close").show(truncate=False)
    df = create_xau_json_dataframe(spark)
    df.withColumn("date_price", F.get_json_object(F.col("value"), "$.data[0]")) \
        .select("date_price") \
        .show(truncate=False)


def multiple_field_manipulation(spark: SparkSession) -> None:
    df = create_cad_json_dataframe(spark)
    df.select(
        F.json_tuple("Time Series FX (Daily)", "2026-04-08", "2026-04-07")
        .alias("April_08_2026", "April_07_2026")) \
        .show(truncate=False)

def json_string_to_structure(spark: SparkSession) -> None:
    df = create_xau_json_dataframe(spark)
    schema = T.StructType([
        T.StructField("nominal", T.StringType(), True),
        T.StructField("data", T.StringType(), True),
    ])
    df = df.withColumn("parsed", F.from_json("value", schema))
    df = df.withColumn("nominal", df["parsed.nominal"]).withColumn("data", df["parsed.data"])
    df.show()

def infer_schema(spark: SparkSession) -> None:
    df = create_xau_json_dataframe(spark)
    json_string = df.select(F.col("value")).first()[0]
    schema = F.schema_of_json(F.lit(json_string))
    df = df.withColumn("parsed", F.from_json("value", schema))
    df.show()

def structure_to_json(spark: SparkSession) -> None:
    df = create_xau_json_dataframe(spark)
    json_string = df.select(F.col("value")).first()[0]
    schema = F.schema_of_json(F.lit(json_string))
    df = df.withColumn("parsed", F.from_json("value", schema)).select(F.col("parsed").alias("struct_data"))
    df.withColumn("struct_data", F.to_json(F.struct("struct_data.data.date", "struct_data.data.price"))).show()
    df = create_cad_df_open_close_price(spark)
    df.withColumn("json_string", F.to_json(F.struct([c for c in df.columns]))).show(truncate=False)

def json_flatten(spark: SparkSession) -> None:
    df = create_xau_json_dataframe(spark)
    json_string = df.select(F.col("value")).first()[0]
    schema = F.schema_of_json(F.lit(json_string))
    df = df.withColumn("parsed", F.from_json("value", schema)).select(F.col("parsed"))
    df = df.withColumn(
        "zipped_data",
        F.arrays_zip("parsed.data.date", "parsed.data.price")
    )
    df = df.select("parsed.nominal", F.explode("zipped_data").alias("item"))
    df.select('nominal', 'item.date', 'item.price').show()









