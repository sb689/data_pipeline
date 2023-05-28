import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum


default_args = {
    'owner': 'sb',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag_stock_analysis = DAG(
    dag_id="stock_etf_analysis_dag_v_06",
    default_args=default_args,
    schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=300),
    description='reads stock data, transforms and writes into parquet file',
    start_date=pendulum.now()
)
stage1_stocks_transform_task = SparkSubmitOperator(
    application='./airflow/data_pipeline/pipeline/dags/transform_stocks_stage1.py',
    conn_id='spark_stand_alone',
    task_id='stage1_stocks_transform_task',
    dag=dag_stock_analysis
)

stage2_stocks_transform_task = SparkSubmitOperator(
    application='./airflow/data_pipeline/pipeline/dags/transform_stocks_stage2.py',
    conn_id='spark_stand_alone',
    task_id='stage2_stocks_transform_task',
    dag=dag_stock_analysis
)

stage1_etfs_transform_task = SparkSubmitOperator(
    application='./airflow/data_pipeline/pipeline/dags/transform_etfs_stage1.py',
    conn_id='spark_stand_alone',
    task_id='stage1_etfs_transform_task',
    dag=dag_stock_analysis
)

stage2_etfs_transform_task = SparkSubmitOperator(
    application='./airflow/data_pipeline/pipeline/dags/transform_etfs_stage2.py',
    conn_id='spark_stand_alone',
    task_id='stage2_etfs_transform_task',
    dag=dag_stock_analysis
)

stage3_ml_task = SparkSubmitOperator(
    application='./airflow/data_pipeline/pipeline/dags/ml_integrate_stage3.py',
    conn_id='spark_stand_alone',
    task_id='stage3_ml_task',
    dag=dag_stock_analysis
)

stage1_stocks_transform_task >> stage2_stocks_transform_task 
stage1_etfs_transform_task >> stage2_etfs_transform_task 
stage2_stocks_transform_task >> stage3_ml_task
stage2_etfs_transform_task >> stage3_ml_task
