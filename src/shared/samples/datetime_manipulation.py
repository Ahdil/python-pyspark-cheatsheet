from data.data_generation import *

logger = logging.getLogger(__name__)
PATH = Path(__file__).parents[3] / "data"


def string_to_date(spark: SparkSession) -> None:
    df = create_xau_df_date_as_string(spark)
    df = df.withColumn("date", F.to_date("date", "yyyy-MM-dd")) \
        .withColumn("sml_month", F.date_format("date", "dd-MMM-yyyy")) \
        .withColumn("parsed_sml_month", F.to_date("sml_month", "dd-MMM-yyyy")) \
        .withColumn("slashed", F.date_format("date", "MM/dd/yyyy")) \
        .withColumn("parsed_slashed", F.to_date("date", "mm/dd/yyyy"))
    df.printSchema()
    df.show(5)


def string_to_timestamp(spark: SparkSession) -> None:
    format_date = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    df = create_df_json_gcp_tasks(spark, no_schema=True)
    df.withColumn("End", F.to_timestamp("End", format_date)) \
        .withColumn("Started", F.to_timestamp("Started", format_date)).show(truncate=False)


def date_to_string(spark: SparkSession) -> None:
    df = create_xau_df_date_as_string(spark)
    df = df.withColumn('date', F.to_date("date", "yyyy-MM-dd"))
    df.withColumn("date01", F.date_format("date", "yyyy-MM-dd")) \
        .withColumn("date02", F.date_format("date", "MMMM dd, yyyy")) \
        .withColumn("date03", F.date_format("date", "EEE, MMMM dd, yyyy")) \
        .show()


def timestamp_to_string(spark: SparkSession) -> None:
    df = create_df_json_gcp_tasks(spark)
    df = df.select(F.col("End").alias("original_date"))
    df.withColumn('date01', F.date_format("original_date", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
        .withColumn('date02', F.date_format("original_date", "EEE, yyyy-MM-dd HH:mm:ss")) \
        .show(truncate=False)


def date_functions(spark: SparkSession) -> None:
    df = spark.createDataFrame([
        {"original_date": date(2026, 1, 1)},
        {"original_date": date(2026, 2, 1)},
        {"original_date": date(2026, 3, 1)},
    ])
    df.withColumn("current_date", F.current_date()) \
        .withColumn("10DaysLater", F.date_add("original_date", 10)) \
        .withColumn("5DaysEarlier", F.date_sub("original_date", 5)) \
        .withColumn("DaysFromToday", F.date_diff(F.current_date(), "original_date")) \
        .withColumn("6MonthsLater", F.add_months("original_date",6)) \
        .withColumn("Year", F.year("original_date")) \
        .withColumn("Month", F.month("original_date")) \
        .withColumn("Day", F.day("original_date")) \
        .withColumn("DayOfMonth", F.dayofmonth("original_date")) \
        .withColumn("DayOfYear", F.dayofyear("original_date")) \
        .withColumn("DayOfWeek", F.dayofweek("original_date")) \
        .withColumn("WeekOfYear", F.weekofyear("original_date")) \
        .withColumn("FirstDayOfMonth", F.trunc("original_date", "MM")) \
        .withColumn("WeekOfYear", F.weekofyear("original_date")) \
        .withColumn("NextMonday", F.next_day("original_date", "Monday")) \
        .withColumn("LastDayOfMonth", F.last_day("original_date")) \
        .show(truncate=False)


def datetime_functions(spark: SparkSession) -> None:
    df = create_df_json_gcp_tasks(spark)
    df = df.select(F.col("Started").alias("original_time"))
    df.withColumn("current_timestamp", F.current_timestamp()) \
        .withColumn("Hour", F.hour("original_time")) \
        .withColumn("minute", F.minute("original_time")) \
        .withColumn("second", F.second("original_time")) \
        .withColumn("unix", F.unix_timestamp("original_time")) \
        .withColumn("FromUnix", F.from_unixtime("unix")) \
        .show(truncate=False)

