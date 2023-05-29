# data_pipeline

### Datapipline configuration to test

The code base is developed using standalone airflow. To run the data pipeline DAG, copy paste the data_pipeleine folder inside `/airflow. configure airflow.cfg to read dags from '/Users/..../airflow/data_pipeline/pipeline/dags'. Rest should work as is.

### Webservice test

The webserver endpoint is developed and tested in this repo: https://github.com/sb689/server_api

### DAG diagram

<img width="1442" alt="Screen Shot 2023-05-28 at 2 59 47 PM" src="https://github.com/sb689/data_pipeline/assets/21972114/d7f75a76-3255-4f2e-b0cd-e7129e99907b">

### Unit test for problem 2 feature engineering

The file stock_analysis_test.py contains unit test for feature engineering. To run the tests, install the required packages. then execute command 'pytest stock_analysis_test.py'.
