from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, percentile_approx

spark = SparkSession \
    .builder \
    .appName("stock data analysis") \
    .getOrCreate()

# read from parquet file of stage1
out_path = "./airflow/data_pipeline/pipeline/etf-data-out/landing_etfs.parquet"
final_df = spark.read.parquet(out_path)
print("print scema from parquet =====================")
final_df.printSchema()


# Feature engineering
window_spec = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(Window.currentRow - 29, Window.currentRow)
final_df = final_df.withColumn("vol_moving_avg", avg("volume").over(window_spec))
final_df = final_df.withColumn("adj_close_rolling_med", percentile_approx("AdjClose", 0.5).over(window_spec))
print("print scema from final_df =====================")
final_df.printSchema()


# write into parquet format into another directory
transformed_out_path = "./airflow/data_pipeline/pipeline/stock-etf-data-transformed/stock_etf.parquet"
final_df.write.mode("overwrite").save(transformed_out_path, format="parquet")
