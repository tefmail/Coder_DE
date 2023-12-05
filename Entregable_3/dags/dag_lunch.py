from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from scripts.main import load_fact_table, load_dim_tables

default_args={
    'retries':5,
    'retry_delay':timedelta(minutes=5)

}

with DAG(
    default_args=default_args,
    dag_id='dag_lunch',
    description= 'Mi dag',
    start_date=datetime(2023,12,2),
    schedule_interval='@daily',
    catchup=False
    ) as dag:

    # task con dummy operator
    dummy_start_task = DummyOperator(
        task_id="start"
    )

    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="coderhouse_redshift",
        sql="sql/creates.sql",
        hook_params={
            "options": "-c search_path=tefmail_coderhouse_schema"
        }
    )

    # load fact tables
    task1=PythonOperator(
        task_id='load_fact_tables',
        python_callable=load_fact_table,

    )

    # load dim tables
    task1=PythonOperator(
        task_id='load_dim_tables',
        python_callable=load_dim_tables,

    )
    dummy_end_task = DummyOperator(
        task_id="end"
    )

    dummy_start_task >> create_tables_task
    create_tables_task >> load_fact_tables
    create_tables_task >> load_dim_tables

    load_fact_tables >> dummy_end_task
    load_dim_tables >> dummy_end_task

