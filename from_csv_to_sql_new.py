from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pandas as pd
import os

# Определите базовую директорию
BASE_DIR = os.path.expanduser('~/airflow/datasets_from_net')

def my_python_function():
    import pandas as pd
    # Используйте абсолютный путь
    df = pd.read_csv(f'{BASE_DIR}/for_datalens.csv')
    df = df.loc[df['Country_store']!='Deutschland']
    df.to_csv(f'{BASE_DIR}/for_datalens_processed.csv')

default_args = {
    'owner': 'gornostaev',
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
}

with DAG(
    dag_id="simple_sql_python_bash_dag_fixed",
    default_args=default_args,
    start_date=datetime(2025, 10, 21),
    schedule_interval=None,
    catchup=False,
    tags=["example"],
) as dag:
    
    # Создаем директорию заранее
    create_dir = BashOperator(
        task_id="create_directory",
        bash_command=f"mkdir -p {BASE_DIR}"
    )

    download_task = BashOperator(
        task_id="wget_file",
        bash_command=f"wget -O {BASE_DIR}/for_datalens.csv http://0.0.0.0:7000/for_datalens.csv",
    )

    python_task = PythonOperator(
        task_id="run_python_code",
        python_callable=my_python_function,
    )

    create_auto_table = SQLExecuteQueryOperator(
        task_id='create_auto_table',
        conn_id='duck_fashion',
        sql=f"""
            CREATE OR REPLACE TABLE analytics_data AS
            SELECT *
            FROM read_csv('{BASE_DIR}/for_datalens_processed.csv');
        """
    )

    create_dir >> download_task >> python_task >> create_auto_table