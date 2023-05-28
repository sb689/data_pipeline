
from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("stock data analysis") \
    .getOrCreate()


# read from parquet file of stage1
out_path = "./airflow/data_pipeline/pipeline/stock-data-out/landing_stocks.parquet"
stock_out_df = spark.read.parquet(out_path)
print("print scema after parquet conversion =====================")
stock_out_df.printSchema()


# Feature engineering
stock_out_df.createOrReplaceTempView("stock_table")

vol_moving_avg_df = spark.sql('''
          SELECT *,
          avg(volume) OVER(
              PARTITION BY Symbol
              ORDER BY Date
              ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
              AS vol_moving_avg
          FROM stock_table
          '''
                              )

vol_moving_avg_df.createOrReplaceTempView("stock_table")

final_df = spark.sql('''
          SELECT *,
          avg(AdjClose) OVER(
              PARTITION BY Symbol
              ORDER BY Date
              ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
              AS adj_close_rolling_med
          FROM stock_table
          '''
                     )
print("final_df =====================")


# write into parquet format into another directory
transformed_out_path = "./airflow/data_pipeline/pipeline/stock-etf-data-transformed/stock_etf.parquet"
final_df.write.mode("overwrite").save(transformed_out_path, format="parquet")
