from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'dbt_run_pipeline',
    default_args=default_args,
    description='Run dbt transformations on Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2025, 6, 28),
    catchup=False,
) as dag:

    dim_customer = BashOperator(
        task_id='dim_customer',
        bash_command='cd "D:\Data Engineer\test_project\snowflake_transform" && dbt run --select dim_customer',
    )

    dim_region = BashOperator(
        task_id='dim_region',
        bash_command='cd "D:\Data Engineer\test_project\snowflake_transform" && dbt run --select dim_region',
    )

    fact_sales = BashOperator(
        task_id='fact_sales',
        bash_command='cd "D:\Data Engineer\test_project\snowflake_transform" && dbt run --select fact_sales',
    )

    region_wise = BashOperator(
        task_id='region_wise',
        bash_command='cd "D:\Data Engineer\test_project\snowflake_transform" && dbt run --select region_wise',
    )
    total_orders = BashOperator(
        task_id='total_orders',
        bash_command='cd "D:\Data Engineer\test_project\snowflake_transform" && dbt run --select total_orders',
    )

    dim_customer >> dim_region >> fact_sales >> region_wise >> total_orders