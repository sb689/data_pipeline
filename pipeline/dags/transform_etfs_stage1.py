from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, input_file_name, regexp_extract


spark = SparkSession \
    .builder \
    .appName("stock data analysis") \
    .getOrCreate()

schema = StructType() \
    .add("Date", StringType(), True) \
    .add("Open", FloatType(), True) \
    .add("High", FloatType(), True) \
    .add("Low", FloatType(), True) \
    .add("Close", FloatType(), True) \
    .add("Adj Close", FloatType(), True) \
    .add("Volume", IntegerType(), True)

path = "./airflow/data_pipeline/pipeline/stock-data/symbols_valid_meta.csv"
stock_df_selected_columns = spark.read.option("header", "true").csv(path)
stock_df_selected_columns = stock_df_selected_columns.select(
    ["Symbol", "Security Name"])

# Stocks
path = "./airflow/data_pipeline/pipeline/stock-data/etfs/"

etf_df = spark.read.option("header", "true").schema(schema).csv(path) \
    .withColumn("Symbol1", input_file_name()) \
    .withColumnRenamed("Adj Close", "AdjClose")

etf_df = etf_df.withColumn(
    "Symbol2", regexp_extract(col('Symbol1'), r'^.*[\\/](.+?)\.[^.]+$', 1))

etf_df = etf_df.drop(col("Symbol1"))

etf_df = etf_df.join(stock_df_selected_columns,
                     etf_df.Symbol2 == stock_df_selected_columns.Symbol, "left")
etf_df = etf_df.drop(col("Symbol2"))

# write into parquet format
out_path = "./airflow/data_pipeline/pipeline/etf-data-out/landing_etfs.parquet"
etf_df.write.mode("overwrite").save(out_path, format="parquet")
