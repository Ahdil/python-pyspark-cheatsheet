import requests, json, logging, os
import pyspark.sql.functions as F

import config.settings as S

from pathlib import Path
from pyspark.sql.types import MapType, StringType, StructField, StructType
from pyspark.sql.types import DateType, DecimalType, ArrayType, TimestampType
from pyspark.sql import SparkSession, DataFrame
from dotenv import load_dotenv
from datetime import date, datetime
import os
import shutil
import glob


logger = logging.getLogger(__name__)
FOLDER_PATH = Path(__file__).parent
load_dotenv()
TEMP_PATH = Path("/") / "python_temp"

def rebuild_unique_file(path:str, ext:str = "csv") -> None:
    try:
        search_pattern = str(TEMP_PATH / f"part*.{ext}")
        csv_files = glob.glob(search_pattern)
        if not csv_files:
            logger.error(f"No csv files found at {search_pattern}")
            return

        csv_file = csv_files[0]
        os.replace(csv_file, path)
        logger.info(f"Successfully moved to  {path}")

    except Exception as e:
        logger.error(f"Error when moving file: {e}")
    finally:
        if os.path.exists(TEMP_PATH):
            shutil.rmtree(TEMP_PATH)

def generate_data_as_list_of_rows() -> list:
    return [("Alice", 34, date(1992, 1, 19), datetime.now(), False),
            ("Bob", 45, date(1981, 9, 30), datetime.now(), True),
            ("Charlie", 29, date(1997, 4, 5), datetime.now(), True)]

def generate_data_as_list_of_dictionaries() -> list:
    return [
        {"name": "Alice", "age": 34, "date": date(1992, 1, 19), "logged": datetime.now(), "active": False},
        {"name": "Bob", "age": 45, "date": date(1981, 9, 30), "logged": datetime.now(), "active": True},
        {"name": "Charlie", "age": 29, "date": date(1997, 4, 5), "logged": datetime.now(), "active": True}
    ]

def get_json_todos() -> Path | None:
    file_path = FOLDER_PATH / "todos.json"
    if file_path.exists():
        return file_path
    try:
        url = S.Settings.JSON_MOCK_DATA_BASIC_URL + "/todos"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=4)
        return file_path
    except Exception as err:
        logger.error(f"Error generating JSON file: {err}")
        return None

def get_json_xau_historical_price() -> Path | None:
    file_path = FOLDER_PATH / str(date.today()) / "xau.json"
    if file_path.exists():
        return file_path
    try:
        url = S.Settings.JSON_ALPHAVANTAGE_BASIC_URL
        params = {
            "function": "GOLD_SILVER_HISTORY",
            "symbol": "XAU",
            "interval": "daily",
            "apikey": os.getenv("JSON_ALPHAVANTAGE_API_KEY")
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=4)
        return file_path
    except Exception as err:
        logger.error(f"Error generating JSON file: {err}")
        return None

def get_json_cad_historical_price() -> Path | None:
    file_path = FOLDER_PATH / str(date.today()) / "cad.json"
    if file_path.exists():
        return file_path
    try:
        url = S.Settings.JSON_ALPHAVANTAGE_BASIC_URL
        params = {
            "function": "FX_DAILY",
            "from_symbol": "USD",
            "to_symbol": "CAD",
            "apikey": os.getenv("JSON_ALPHAVANTAGE_API_KEY")
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=4)
        return file_path
    except Exception as err:
        logger.error(f"Error generating JSON file: {err}")
        return None

def create_xau_dataframe(spark) -> DataFrame:
    xau_path = get_json_xau_historical_price()
    schema = StructType([
        StructField("data", ArrayType(
            StructType([
                StructField("date", DateType(), True),
                StructField("price", DecimalType(10, 4), True)
            ])
        ), True)
    ])
    df = spark.read.option("multiline", True).schema(schema).json(str(xau_path))
    return df.select(F.explode(F.col("data")).alias("row")).select("row.date", "row.price")

def create_cad_dataframe(spark) -> DataFrame:
    cad_path = get_json_cad_historical_price()
    schema = StructType([
        StructField
            (
            "Time Series FX (Daily)",
            MapType(StringType(),
                    MapType(StringType(), DecimalType(10, 4))),
            True
        )
    ])
    df = spark.read.option("multiline", True).schema(schema).json(str(cad_path))
    return df.select(F.explode(F.col("Time Series FX (Daily)"))) \
        .select(F.col("key").cast("date").alias('date'), F.col("value")["4. close"].alias("price"))

def create_xau_df_date_as_string(spark: SparkSession) -> DataFrame:
    path = get_json_xau_historical_price()
    schema = StructType([
        StructField("data",
                    ArrayType(
                        StructType([
                            StructField("date", StringType(), True),
                            StructField("price", DecimalType(10, 4), True)
                        ]),
                        True)
                    )
    ])
    return spark.read.option("multiline", True).json(str(path), schema=schema) \
        .select(F.explode("data").alias("row")).select("row.date", "row.price")


def create_df_json_gcp_tasks(spark: SparkSession, no_schema=False) -> DataFrame:
    file_path = FOLDER_PATH / "tasks.json"
    if no_schema:
        return spark.read.json(str(file_path))
    schema = StructType([
        StructField("List", StringType()),
        StructField("Title", StringType()),
        StructField("End", TimestampType()),
        StructField("Started", TimestampType()),
    ])
    return spark.read.schema(schema).json(str(file_path))


def create_cad_df_open_close_price(spark: SparkSession) -> DataFrame:
    path = get_json_cad_historical_price()
    schema = StructType([
        StructField("Time Series FX (Daily)", MapType(
            StringType(), MapType(StringType(), DecimalType(10, 4))
        ))
    ])
    df = spark.read.option("multiline", True).schema(schema).json(str(path))
    df = df.select(F.explode(F.col("Time Series FX (Daily)"))) \
        .select(F.col("key").cast("date").alias("date"),
                F.col("value")["4. close"].alias("close"),
                F.col("value")["1. open"].alias("open"), )
    return df.withColumn("diff", F.col("close") - F.col("open"))


def get_working_df(spark: SparkSession) -> DataFrame:
    path = FOLDER_PATH / "adventure_sales.parquet"
    if os.path.exists(path):
        return spark.read.parquet(str(path))

    config = S.AzureConfig()
    columns = ["P.Name AS Product", "M.Name AS Model", "P.ListPrice", "S.UnitPrice", "S.SalesOrderDetailID",
               "S.UnitPriceDiscount", "S.OrderQty", "S.LineTotal", "S.ModifiedDate AS OrderDate"]
    query = f"""SELECT {', '.join(columns)} FROM 
                [SalesLT].Product AS P 
                INNER JOIN [SalesLT].[SalesOrderDetail] AS S ON P.ProductId = S.ProductId  
                INNER JOIN [SalesLT].[ProductCategory] AS C ON P.ProductCategoryId = C.ProductCategoryId
                INNER JOIN [SalesLT].[ProductModel] AS M ON P.ProductModelID = M.ProductModelId
            """
    df = (spark.read.format("jdbc")
          .option("url", config.connection_url)
          .option("query", query)
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
          .load()
          )
    df.write.mode("overwrite").parquet(str(TEMP_PATH))
    rebuild_unique_file(str(path), "parquet")
    return df


def create_cad_json_dataframe(spark) -> DataFrame:
    cad_path = get_json_cad_historical_price()
    schema = StructType([
        StructField
            (
            "Time Series FX (Daily)",  StringType(), True
        )
    ])
    return  spark.read.option("multiline", True).schema(schema).json(str(cad_path))

def create_xau_json_dataframe(spark: SparkSession) -> DataFrame:
    xau_path = get_json_xau_historical_price()
    return spark.read.text(paths=str(xau_path), wholetext=True)


