from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.python_operator  import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from scripts.main import load_fact_table, load_dim_tables, enviar_alerta
from scripts.utility import *

default_args={
    'retries':2,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id='dag_lunch',
    description= 'Mi dag',
    start_date=datetime(2023,12,2),
    schedule_interval='0 */12 * * *',  #'@daily',
    catchup=False,
    on_success_callback=enviar
    ) as dag:

    # task con dummy operator
    dummy_start_task = DummyOperator(
        task_id="start"
    )

    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="coderhouse_redshift",
        sql="sql/creates.sql",
        #database = "data-engineer-database",
        hook_params={"options": "-c search_path=tefmail_coderhouse"}
    )

    # load fact tables
    task1=PythonOperator(
        task_id='load_fact_tables',
        python_callable=load_fact_table,
        op_kwargs={"config_path": "/opt/airflow/config/config.ini"}
    )

    task4=PythonOperator(
        task_id='alerta_smtp',
        python_callable=enviar_alerta,
        op_kwargs={"config_path": "/opt/airflow/config/config.ini"}
        )

    # load dim tables
    task2=PythonOperator(
        task_id='load_dim_tables',
        python_callable=load_dim_tables,
        op_kwargs={"config_path": "/opt/airflow/config/config.ini"}
    )

    task3=PythonOperator(
        task_id='smtp',
        python_callable=enviar,
        )

    dummy_end_task = DummyOperator(
        task_id="end"
    )

    dummy_start_task >> create_tables_task
    create_tables_task >> task1 
    create_tables_task >> task2

    task1 >> task3
    task1 >> task4
    task2 >> task3

    task4 >> dummy_end_task
    task3 >> dummy_end_task