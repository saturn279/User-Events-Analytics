from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta

default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_data_from_s3_to_postgres',
    default_args=default_args,
    schedule_interval='0 * * * *',
)


s3_to_postgres = SparkSubmitOperator(task_id='s3_to_postgres',
conn_id='spark_local',
application=f'{pyspark_app_home}/spark/S3toPostgres.py',
total_executor_cores=4,
executor_cores=2,
executor_memory='5g',
driver_memory='5g',
name='s3_to_postgres',
execution_timeout=timedelta(minutes=59),
dag=dag
)

s3_to_postgres