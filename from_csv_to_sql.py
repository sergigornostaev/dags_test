from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



def my_python_function():
    import pandas as pd
    df=pd.read_csv(r'~/datasets_from_net/for_datalens.csv')

    df=df.loc[df['Country_store']!='Deutschland']

    df.to_csv(r'~/datasets_from_net/for_datalens_processed.csv')

# Определение DAG
default_args = {
    'owner': 'gornostaev',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="simple_sql_python_bash_dag",
    default_args=default_args,
    start_date=datetime(2025, 10, 21),
    schedule_interval=None,
    catchup=False,
    tags=["example"],
) as dag:
    start_task = BashOperator(
        task_id="wget_file",
        bash_command="mkdir -p ~/datasets_from_net && wget -P ~/datasets_from_net http://0.0.0.0:7000/for_datalens.csv",
    )

    python_task = PythonOperator(
        task_id="run_python_code",
        python_callable=my_python_function,
    )


    create_auto_table = SQLExecuteQueryOperator(
        task_id='create_auto_table',
        conn_id='duck_fashion',
        sql="""
            CREATE OR REPLACE TABLE analytics_data AS
            SELECT *
            FROM read_csv('~/datasets_from_net/for_datalens_processed.csv');
        """
    )


    


    start_task >> python_task >> create_auto_table


