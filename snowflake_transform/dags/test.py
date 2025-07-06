
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'dbt_run_test',
    default_args=default_args,
    description='Run dbt transformations on Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2025, 6, 28),
    catchup=False,
) as dag:
    
     testing = BashOperator(
        task_id='dim_customer',
        bash_command='cd /opt/airflow/test_project/snowflake_transform && ls',
    )
testing