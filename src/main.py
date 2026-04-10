from src.shared.samples.df_creation import *
# from src.shared.samples.file_reading import *
# from src.shared.samples.file_writing import *
# from src.shared.samples.columns_manipulation import *
# from src.shared.samples.df_filtering import *
# from src.shared.samples.df_grouping import *
# from src.shared.samples.df_joining import *
# from src.shared.samples.datetime_manipulation import *
# from src.shared.samples.math_functions import *
# from src.shared.samples.string_functions import *
# from src.shared.samples.window_functions import  *
# from src.shared.samples.array_functions import  *
# from src.shared.samples.json_functions import  *
# from src.shared.samples.sql_queries import *
from pathlib import Path
from src.utils.spark_utils import get_spark_session

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)
PATH = Path(__file__).parents[1] / "data"


def main():
    spark = get_spark_session()
    # ------Uncomment df_creation import
    create_df_with_default_schema(spark)
    create_df_with_explicit_struct_type_schema(spark)
    create_df_with_explicit_struct_type_schema_dates(spark)
    create_df_with_explicit_sql_schema(spark)
    create_df_with_inferred_dictionary(spark)

    # ------Uncomment file_reading
    # read_csv_file_inferred(spark)
    # read_csv_file(spark)
    # read_csv_file_with_type(spark)
    # read_json_file(spark)
    # read_json_file_with_explicit_schema(spark)

    # ------Uncomment file_writing
    # basic_csv_writer(spark, str(PATH / 'users.csv'))
    # with_options_csv_writer(spark, str(PATH / 'users.csv'))
    # basic_json_writer(spark, str(PATH / 'users.json'))
    # basic_parquet_writer(spark, str(PATH / 'users.parquet'))
    # write_json_parquet(spark, str(PATH / 'todos.parquet'))

    # ------Uncomment columns_manipulation
    # column_selector(spark)
    # column_selector(spark, False)
    # column_rename(spark)
    # add_columns(spark)
    # drop_columns(spark)

    # ------Uncomment df_filtering
    # basic_filter(spark)
    # multiple_conditions_filter(spark)
    # string_filter(spark)
    # null_filter(spark)
    # from_list_filter(spark)
    # data_cleansing_filter(spark)

    # ------Uncomment df_grouping
    # basic_grouping(spark)
    # aggregate_grouping(spark)

    # ------Uncomment df_joining
    # basic_join(spark)
    # left_join(spark)
    # right_join(spark)
    # outer_join(spark)
    # left_anti_join(spark)
    # cross_join(spark)

    # ------Uncomment datetime_manipulation
    # string_to_date(spark)
    # string_to_timestamp(spark)
    # date_to_string(spark)
    # timestamp_to_string(spark)
    # date_functions(spark)
    # datetime_functions(spark)

    # ------Uncomment math_functions
    # simple_arith_func(spark)
    # complex_arith_func(spark)

    # ------Uncomment string_functions
    # basic_string_func(spark)
    # trim_pad_functions(spark)
    # extracting_functions(spark)
    # converting_functions(spark)

    # ------Uncomment window_functions
    # basic_functions(spark)
    # rows_functions(spark)

    # ------Uncomment array_functions
    # array_manipulation(spark)
    # array_elements(spark)
    # array_modification(spark)
    # array_to_row(spark)
    # rows_to_array(spark)

    # ------Uncomment json_functions
    # single_field_manipulation(spark)
    # multiple_field_manipulation(spark)
    # json_string_to_structure(spark)
    # infer_schema(spark)
    # structure_to_json(spark)
    # json_flatten(spark)

    # ------Uncomment sql_queries
    # with_temp_view(spark)
    # without_temp_view(spark)

    spark.stop()


if __name__ == "__main__":
    main()
