from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pandas as pd




# Определение DAG
default_args = {
    'owner': 'gornostaev',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="test_bash_dag",
    start_date=datetime(2025, 10, 21),
    schedule_interval=None,
    catchup=False,
    tags=["example"],
) as dag:
    start_task = BashOperator(
        task_id="wget_file",
        bash_command="mkdir -p ./datasets_from_net && wget -P ./datasets_from_net http://0.0.0.0:7000/for_datalens.csv",
    )




