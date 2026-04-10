import logging
from pyspark.sql import SparkSession, functions as F
from data.data_generation import get_working_df

logger = logging.getLogger(__name__)


def array_manipulation(spark: SparkSession) -> None:
    df = get_working_df(spark)
    # logger.info(f"Total rows {df.count()}")
    df.withColumn('List_Unit', F.array("ListPrice", "UnitPrice")) \
        .withColumn('Array_Size', F.size("List_Unit")) \
        .withColumn('Array_Sorted', F.array_sort("List_Unit")) \
        .withColumn('Array_Contains', F.array_contains("List_Unit", 356.8980)) \
        .show(542, False)


def array_elements(spark: SparkSession) -> None:
    df = get_working_df(spark)
    df.withColumn('List_Unit', F.array("Model", "ListPrice", "UnitPrice")) \
        .withColumn('SelectElement1', F.element_at("List_Unit", 1)) \
        .withColumn("PythonElement0", F.col("List_Unit").getItem(0)) \
        .show(542, False)


def array_modification(spark: SparkSession) -> None:
    df = get_working_df(spark)
    df = df.withColumn('DiscountedPrice', (F.col('UnitPrice') * (1 - F.col('UnitPriceDiscount'))).cast('decimal(19,4)')) \
        .withColumn('Index', F.monotonically_increasing_id()) \
        .select('Index', 'Model', 'OrderQty', 'UnitPrice', 'ListPrice', 'DiscountedPrice')

    df.withColumn('ToArray', F.array('Model', 'OrderQty', 'UnitPrice', 'ListPrice', 'DiscountedPrice')) \
        .withColumn('RemoveValue', F.array_remove('ToArray', 'ML Road Frame-W')) \
        .withColumn('DistinctArray', F.array_distinct('ToArray')) \
        .withColumn('MergeArray', F.array_union('ToArray', 'ToArray')) \
        .select('Index', 'ToArray', 'RemoveValue', 'DistinctArray', 'MergeArray') \
        .show(542, False)


def array_to_row(spark: SparkSession) -> None:
    df = get_working_df(spark).select('Product', 'UnitPrice') \
        .groupBy('Product').agg(F.array_sort(F.collect_list('UnitPrice')).alias('UnitPrices'))
    df.withColumn('UnitPrice', F.explode(F.col('UnitPrices'))).show(542, False)
    df.select('Product',F.posexplode('UnitPrices').alias("pos", "price")).show(542, False)



def rows_to_array(spark: SparkSession) -> None:
    df = get_working_df(spark)
    df_list = df.select('Product', 'UnitPrice')
    df_list.groupBy('Product').agg(F.array_sort(F.collect_list('UnitPrice')).alias('UnitPrices')) \
        .show(542, False)
    df_list.groupBy('Product').agg(F.array_sort(F.collect_set('UnitPrice')).alias('UnitPrices')) \
        .show(542, False)
