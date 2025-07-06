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
        bash_command=r'dbt run --select dim_customer  --profiles-dir /home/airflow/.dbt --project-dir /opt/airflow/test_project/snowflake_transform',
    )

    dim_region = BashOperator(
        task_id='dim_region',
        bash_command=r'dbt run --select dim_region  --profiles-dir /home/airflow/.dbt --project-dir /opt/airflow/test_project/snowflake_transform',
    )

    fact_sales = BashOperator(
        task_id='fact_sales',
        bash_command=r'dbt run --select fact_sales  --profiles-dir /home/airflow/.dbt --project-dir /opt/airflow/test_project/snowflake_transform',
    )

    region_wise = BashOperator(
        task_id='region_wise',
        bash_command=r'dbt run --select region_wise  --profiles-dir /home/airflow/.dbt --project-dir /opt/airflow/test_project/snowflake_transform',
    )
    total_orders = BashOperator(
        task_id='total_orders',
        bash_command=r'dbt run --select total_orders  --profiles-dir /home/airflow/.dbt --project-dir /opt/airflow/test_project/snowflake_transform',
    )

    dim_customer >> dim_region >> fact_sales >> region_wise >> total_orders