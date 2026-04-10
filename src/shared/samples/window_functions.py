import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from data.data_generation import get_working_df

logger = logging.getLogger(__name__)


def basic_functions(spark: SparkSession) -> None:
    df = get_working_df(spark)
    # logger.info(f"Total rows {df.count()}")
    window_frame = Window.partitionBy("Model").orderBy(F.col("OrderQty").desc())
    df.withColumn('row_number', F.row_number().over(window_frame)) \
        .withColumn('rank', F.rank().over(window_frame)) \
        .withColumn('dense_rank', F.dense_rank().over(window_frame)) \
        .withColumn('previous_price', F.lag('OrderQty').over(window_frame)) \
        .withColumn('next_price', F.lead('OrderQty').over(window_frame)) \
        .withColumn('moving_total', F.sum('OrderQty').over(window_frame)) \
        .withColumn('moving_avg', F.avg('OrderQty').over(window_frame)).show(542, False)


def rows_functions(spark: SparkSession) -> None:
    df = get_working_df(spark)
    window_frame = Window.partitionBy("Model").orderBy("OrderQty")
    df.withColumn('sum_last_two', F.sum("OrderQty").over(window_frame.rowsBetween(-2, 0))) \
        .withColumn('avg_prev_next', F.avg("OrderQty").over(window_frame.rowsBetween(-1, 1))) \
        .withColumn('min_next_two', F.min("OrderQty").over(window_frame.rowsBetween(0, 2))) \
        .withColumn('max_last_all',
                    F.max('OrderQty').over(window_frame.rowsBetween(Window.unboundedPreceding, 0))) \
        .withColumn('count_model',
                    F.count('OrderQty').over(
                        window_frame.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
        .show(542, False)
