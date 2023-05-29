from pyspark.sql import SparkSession, DataFrame
import pytest
from pyspark.sql.window import Window
from pyspark.sql import functions as F


# Get one spark session for the whole test session
@pytest.fixture(scope="session")
def spark_session():
    print("spar_session called")
    return SparkSession \
        .builder \
        .appName("stock data analysis unit test") \
        .getOrCreate()


def test_vol_avg_rolling_median(spark_session):

    print("test_vol_calculation called")
    # Set up test data
    columns = ["Date", "Symbol", "Volume", "AdjClose"]
    columns2 = ["Date", "Symbol", "Volume", "AdjClose",
                "vol_moving_avg", "adj_close_rolling_med"]
    test_data = [('2023-01-01', "AA", 100, 1.5),
                 ('2023-01-02', "AA", 200, 2.5),
                 ('2023-01-03', "AA", 300, 3.5),
                 ('2023-01-04', "AA", 100, 1.5),
                 ('2023-01-05', "BB", 100, 1.5),
                 ('2023-01-06', "BB", 200, 2.5),
                 ('2023-01-07', "BB", 300, 3.5),
                 ('2023-01-08', "BB", 100, 1.5)
                 ]

    # Set up expected data
    expected_data = [('2023-01-01', "AA", 100, 1.5, 100, 1.5),
                     ('2023-01-02', "AA", 200, 2.5, 150, 1.5),
                     ('2023-01-03', "AA", 300, 3.5, 200, 2.5),
                     ('2023-01-04', "AA", 100, 1.5, 175, 1.5),
                     ('2023-01-05', "BB", 100, 1.5, 100, 1.5),
                     ('2023-01-06', "BB", 200, 2.5, 150, 1.5),
                     ('2023-01-07', "BB", 300, 3.5, 200, 2.5),
                     ('2023-01-08', "BB", 100, 1.5, 175, 1.5)]

    # Create dataframes
    final_df: DataFrame = spark_session.createDataFrame(test_data, columns)
    expected_df: DataFrame = spark_session.createDataFrame(
        expected_data, columns2)

    # Transformations

    # Define the window specification
    window_spec = Window.partitionBy("Symbol").orderBy(
        "Date").rowsBetween(Window.currentRow - 3, Window.currentRow)

    # Calculate the moving average using the window specification
    final_df = final_df.withColumn(
        "vol_moving_avg", F.avg("volume").over(window_spec))
    final_df = final_df.withColumn(
        "adj_close_rolling_med", F.percentile_approx("AdjClose", 0.5).over(window_spec))

    rows = final_df.collect()
    expected_rows = expected_df.collect()

   # Compare dataframes row by row
    for row_num, row in enumerate(rows):
        assert row == expected_rows[row_num]


def test_parquet_file_write(spark_session):

    columns = ["Date", "Symbol", "Volume", "AdjClose",
               "vol_moving_avg", "adj_close_rolling_med"]

    # Set up expected data
    expected_data = [('2023-01-01', "AA", 100, 1.5, 100, 1.5),
                     ('2023-01-02', "AA", 200, 2.5, 150, 1.5),
                     ('2023-01-03', "AA", 300, 3.5, 200, 2.5),
                     ('2023-01-04', "AA", 100, 1.5, 175, 1.5),
                     ('2023-01-05', "BB", 100, 1.5, 100, 1.5),
                     ('2023-01-06', "BB", 200, 2.5, 150, 1.5),
                     ('2023-01-07', "BB", 300, 3.5, 200, 2.5),
                     ('2023-01-08', "BB", 100, 1.5, 175, 1.5)]

    expected_df: DataFrame = spark_session.createDataFrame(
        expected_data, columns)

    out_path = "./file_test/data.parquet"
    expected_df.write.mode("overwrite").save(out_path, format="parquet")

    # read from parquet and compare
    read_df = spark_session.read.parquet(out_path)

   # Compare number of records in dataframes
    assert read_df.count() == expected_df.count()


def test_symbol_column_extract(spark_session):

    columns = ["Date", "Symbol1", "Volume", "AdjClose"]

    # Set up expected data
    test_data = [('2023-01-01', "/user/test/AA.csv", 100, 1.5),
                 ('2023-01-02', "/user/test/BB.csv", 200, 2.5),
                 ('2023-01-03', "/user/test/AA.csv", 300, 3.5)]

    test_df = spark_session.createDataFrame(
        test_data, columns)

    test_df = test_df.withColumn("Symbol", F.regexp_extract(
        F.col('Symbol1'), r'^.*[\\/](.+?)\.[^.]+$', 1))
    test_df = test_df.drop(F.col("Symbol1"))

    res = test_df.select("Symbol").collect()
    assert (res[0].Symbol == 'AA' and res[1].Symbol == 'BB')
