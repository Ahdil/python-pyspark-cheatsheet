import logging
from pyspark.sql import SparkSession

from data.data_generation import get_working_df

logger = logging.getLogger(__name__)


def with_temp_view(spark: SparkSession) -> None:
    df = get_working_df(spark)
    # df.printSchema()
    df.createOrReplaceTempView("sales_detail")
    spark.sql("SELECT * FROM sales_detail").show()
    spark.sql("SELECT Product, LineTotal FROM sales_detail").show()
    spark.sql("""SELECT Product, OrderQty, LineTotal
                 FROM sales_detail
                 WHERE model = 'Mountain-200'
                 ORDER BY Product""") \
        .show(truncate=False)

    spark.sql("""SELECT Product, sum(OrderQty) AS TotalQty, sum(LineTotal) AS TotalAmount
                     FROM sales_detail
                     WHERE model = 'Mountain-200'
                     GROUP BY Product
                     ORDER BY Product""") \
            .show(truncate=False)

    spark.sql("SELECT * FROM sales_detail LIMIT 20").show()

    spark.sql("""
              SELECT 
                Product
                , SUM(LineTotal) AS TotalAmount
                , SUM(OrderQty) AS TotalQty
                , CASE 
                    WHEN SUM(OrderQty) > 25 THEN 'Very Popular'
                    WHEN SUM(OrderQty) >= 15 THEN 'Selling well'  
                    ELSE
                      'To stock less'  
                  END AS Rate        
              FROM sales_detail
              WHERE model = 'Mountain-200'
              GROUP BY Product
              ORDER BY Product""").show(truncate=False)

def without_temp_view(spark: SparkSession) -> None:
    df = get_working_df(spark)
    # df.printSchema()
    spark.sql("""
              SELECT 
                Product
                , SUM(LineTotal) AS TotalAmount
                , SUM(OrderQty) AS TotalQty
                , CASE 
                    WHEN SUM(OrderQty) > 25 THEN 'Very Popular'
                    WHEN SUM(OrderQty) >= 15 THEN 'Selling well'  
                    ELSE
                      'To stock less'  
                  END AS Rate        
              FROM {sales_detail}
              WHERE model = 'Mountain-200'
              GROUP BY Product
              ORDER BY Product""", sales_detail = df).show(truncate=False)


