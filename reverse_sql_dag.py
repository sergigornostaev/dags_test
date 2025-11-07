from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os

# Определите базовую директорию
BASE_DIR = os.path.expanduser('~/airflow/datasets_from_net')

default_args = {
    'owner': 'gornostaev',
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
}

with DAG(
    dag_id="cleanup_sql_bash_dag",
    default_args=default_args,
    start_date=datetime(2025, 10, 21),
    schedule_interval=None,
    catchup=False,
    tags=["cleanup"],
) as dag:
    
    # 1. Задание на удаление таблицы в DuckDB
    drop_duckdb_table = SQLExecuteQueryOperator(
        task_id='drop_analytics_table',
        conn_id='duck_fashion',
        sql="""
            DROP TABLE IF EXISTS analytics_data;
        """
    )

    # 2. Задание на удаление папки с данными
    # Используем команду rm -rf для рекурсивного удаления папки и ее содержимого
    remove_data_directory = BashOperator(
        task_id="remove_data_directory",
        bash_command=f"rm -rf {BASE_DIR} ~/datasets_from_net"
    )

    # Задания drop_duckdb_table и remove_data_directory не связаны 
    # никакими зависимостями (>>), поэтому они будут выполняться 
    # параллельно при запуске дага.