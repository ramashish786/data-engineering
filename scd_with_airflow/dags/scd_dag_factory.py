from datetime import datetime, timedelta

import pendulum

from airflow import DAG

from airflow.sensors.external_task import ExternalTaskSensor

from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_kpi_of_dag(
        owner: str = None,
        dag_id: str = None,
        pg_target_schema: str = None,
        pg_target_table: str = None,
        index_kpi: str = None,
        pg_environment: str = None,
        airflow_environment: str = None,
        long_description: str = None,
        short_description: str = None,
        start_date: str = None,
        cron_expression: str = None,
        tags: str = None,
        sensors: str = None,
        sql_query: str = None,
) -> DAG:
    """
    Function that generates a standard DAG to obtain a metric.

    All logic is described and commented inside the function.

    Some elements are processed specifically within this function to handle attributes 
    and achieve the desired effect.

    @param owner: Owner of the DAG.
    @param dag_id: Name of the DAG.
    @param pg_target_schema: Target schema in PostgreSQL.
    @param pg_target_table: Target table in PostgreSQL.
    @param index_kpi: KPI ID.
    @param pg_environment: PostgreSQL environment.
    @param airflow_environment: Airflow environment.
    @param long_description: Full description of the report.
    @param short_description: Short description of the report.
    @param start_date: Start date of the DAG.
    @param cron_expression: Cron schedule.
    @param tags: Tags.
    @param sensors: Sensors.
    @param sql_query: SQL query to get the metric.
    @return: Returns a DAG.
    """
    # DAG Configuration
    local_tz = pendulum.timezone('Europe/Moscow')

    # Tables used in DAG
    pg_target_schema = pg_target_schema
    pg_target_table = pg_target_table
    pg_tmp_schema = 'stg'
    pg_tmp_table = f'tmp_{dag_id}_{{{{ data_interval_start.format("YYYY_MM_DD") }}}}'

    # Connector names for GP
    pg_connect = 'test_db_dev' if pg_environment == 'dev' else 'test_db'

    # Placeholder attribute – can be used for handling different scenarios depending on environment
    airflow_environment = airflow_environment

    # The date comes as a string; after parsing we can extract the date and its elements
    parse_date = pendulum.parse(start_date)

    args = {
        'owner': owner,
        'start_date': datetime(parse_date.year, parse_date.month, parse_date.day, tzinfo=local_tz),
        'catchup': True,
        'depends_on_past': True,
        'retries': 3,
        'retry_delay': timedelta(hours=1),
    }

    # Tags come in str format, so we need to parse them and convert to a list
    raw_tags = list(tags.split(','))
    tags_ = []

    for i in raw_tags:
        tags_.append(  # noqa: PERF401
            i.replace("'", "")
            .replace(" ", '')
            .replace("[", "")
            .replace("]", "")
        )

    # Sensors come in str format, so we need to parse them and convert to a list
    if sensors:
        raw_sensors = list(sensors.split(','))
        sensors_ = []
        for i in raw_sensors:
            sensors_.append(  # noqa: PERF401
                i.replace("'", "")
                .replace(' ', '')
                .replace("[", "")
                .replace("]", "")
            )
    else:
        sensors_ = None

    with DAG(
            dag_id=dag_id,
            schedule_interval=cron_expression,
            default_args=args,
            tags=tags_,
            description=short_description,
            concurrency=1,
            max_active_tasks=1,
            max_active_runs=1,
    ) as dag:
        dag.doc_md = long_description

        start = EmptyOperator(
            task_id='start',
        )

        # If sensors are defined, create sensor tasks; otherwise, create a single empty task
        if sensors_:
            sensors_task = [
                ExternalTaskSensor(
                    task_id=f'sensor_{dag}',
                    external_dag_id=dag,
                    allowed_states=['success'],
                    mode='reschedule',
                    timeout=360000,  # sensor runtime
                    poke_interval=600  # frequency of checks
                ) for dag in sensors_
            ]
        else:
            sensors_task = [EmptyOperator(task_id=f'empty_{value}') for value in range(1)]

        drop_tmp_before = PostgresOperator(
            task_id='drop_tmp_before',
            sql=f'''DROP TABLE IF EXISTS {pg_tmp_schema}.{pg_tmp_table}''',
            postgres_conn_id=pg_connect
        )

        create_tmp = PostgresOperator(
            task_id='create_tmp',
            sql=f'''
            CREATE TABLE {pg_tmp_schema}.{pg_tmp_table} AS
            {
                sql_query.format(
                    start_date="{{ data_interval_start.format('YYYY-MM-DD') }}",
                    end_date="{{ data_interval_end.format('YYYY-MM-DD') }}"
                )
            };
            ''',
            postgres_conn_id=pg_connect
        )

        delete_from_target = PostgresOperator(
            task_id='delete_from_target',
            sql=f'''
            DELETE FROM {pg_target_schema}.{pg_target_table}
            WHERE 
                date IN (
                    SELECT 
                        date 
                    FROM 
                        {pg_tmp_schema}.{pg_tmp_table}
                    WHERE
                        kpi_id = {index_kpi}
                    )
            AND kpi_id = {index_kpi}
            ''',
            postgres_conn_id=pg_connect
        )

        insert_from_tmp_to_target = PostgresOperator(
            task_id='insert_from_tmp_to_target',
            sql=f'''
            INSERT INTO {pg_target_schema}.{pg_target_table}("date", value, kpi_id)
            SELECT 
                "date", 
                value, 
                {index_kpi} AS kpi_id 
            FROM 
                {pg_tmp_schema}.{pg_tmp_table}
            ''',
            postgres_conn_id=pg_connect
        )

        drop_tmp_after = PostgresOperator(
            task_id='drop_tmp_after',
            sql=f'''DROP TABLE IF EXISTS {pg_tmp_schema}.{pg_tmp_table}''',
            postgres_conn_id=pg_connect
        )

        end = EmptyOperator(
            task_id='end',
        )

        start >> sensors_task >> drop_tmp_before >> create_tmp >> delete_from_target >> \
        insert_from_tmp_to_target >> drop_tmp_after >> end

    return dag


# Build a DAG from DAG configuration
def generator_of_morning_kpi_dag_to_gp() -> None:
    """
    Function retrieves a list of configs from the DB and generates DAGs 
    based on the `generator_of_morning_kpi_dag_to_gp` function.

    Iterates over configs and calls `create_kpi_of_dag` each time, which returns a DAG.

    @return: None
    """
    pg_hook = PostgresHook(postgres_conn_id='test_db')

    df = pg_hook.get_pandas_df(  # noqa: PD901
        '''
        SELECT 
            kpi_id,
            dag_id,
            "owner",
            sql_query,
            start_date,
            pg_environment,
            airflow_environment,
            short_description_md,
            long_description_md,
            cron,
            sensors,
            tags
        FROM 
            dim_kpi_dag_gen_config
        WHERE 
            is_actual IS TRUE 
        ORDER BY 
            id;
        '''
    )

    for i in range(len(df)):
        create_kpi_of_dag(
            owner=df.iloc[i].owner,
            dag_id=df.iloc[i].dag_id,
            pg_target_schema='public',
            pg_target_table='fct_dm_kpi',
            index_kpi=df.iloc[i].kpi_id,
            pg_environment=df.iloc[i].pg_environment,
            airflow_environment=df.iloc[i].airflow_environment,
            long_description=df.iloc[i].long_description_md,
            short_description=df.iloc[i].short_description_md,
            start_date=df.iloc[i].start_date,
            cron_expression=df.iloc[i].cron,
            tags=df.iloc[i].tags,
            sensors=df.iloc[i].sensors,
            sql_query=df.iloc[i].sql_query,
        )


generator_of_morning_kpi_dag_to_gp()
