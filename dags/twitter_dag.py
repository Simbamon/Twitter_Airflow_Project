from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_etl import run_twitter_etl

default_args = {
    'owner': 'simbamon',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'twitter_dag',
    default_args = default_args,
    start_date=datetime(2023, 4, 20),
    schedule_interval='0 10 * * *',
    catchup=False,
    tags=["twitter_etl"],
    description = "Twitter data ETL"
)

twitter_s3_etl = PythonOperator(
    task_id="twitter_s3_etl",
    python_callable=run_twitter_etl,
    dag=dag
)

twitter_s3_etl