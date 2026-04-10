from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from data.data_generation import *

logger = logging.getLogger(__name__)
PATH = Path(__file__).parents[3] / "data"


def basic_join(spark: SparkSession) -> None:
    df_xau = create_xau_dataframe(spark)
    df_cad = create_cad_dataframe(spark)
    df_xau.join(df_cad, df_xau.date == df_cad.date).show(60, truncate=False)

def left_join(spark: SparkSession) -> None:
    df_xau = create_xau_dataframe(spark)
    df_cad = create_cad_dataframe(spark)
    df_xau.join(df_cad, on = "date", how="left") \
        .select(df_xau.date, df_xau.price.alias("XAUUSD"), df_cad.price.alias("CADUSD")) \
        .show(60, truncate=False)

def right_join(spark: SparkSession) -> None:
    df_xau = create_xau_dataframe(spark)
    df_cad = create_cad_dataframe(spark)
    df_xau.join(df_cad, on="date", how="right") \
        .select(df_xau.date, df_xau.price.alias("XAUUSD"), df_cad.price.alias("CADUSD")) \
        .orderBy(F.desc(df_xau.date)).show(60, truncate=False)

def outer_join(spark: SparkSession) -> None:
    df_xau = create_xau_dataframe(spark)
    df_cad = create_cad_dataframe(spark)
    df_xau.join(df_cad, on="date", how="outer") \
        .select(df_xau.date, df_xau.price.alias("XAUUSD"), df_cad.price.alias("CADUSD")) \
        .orderBy(F.desc(df_xau.date)).show(60, truncate=False)

def left_anti_join(spark: SparkSession) -> None:
    df_xau = create_xau_dataframe(spark)
    df_cad = create_cad_dataframe(spark)
    df_xau.join(df_cad, on="date", how="left_anti") \
        .select(df_xau.date, df_xau.price.alias("XAUUSD")) \
        .show(60, truncate=False)

def cross_join(spark: SparkSession) -> None:
    df_xau = create_xau_dataframe(spark)
    df_cad = create_cad_dataframe(spark)
    df_xau.crossJoin(df_cad).orderBy(F.desc(df_xau.date)).show(60, truncate=False)








