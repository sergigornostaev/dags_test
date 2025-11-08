from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os
from airflow.utils.trigger_rule import TriggerRule

print('cp1')

print('int_rebase')

print('int_rebase_2')

print('int_rebase_3')


print('int_rebase_6')

print('int_rebase_7')

print('int_rebase_8')

print('int_rebase_9')

print('int_rebase_10')

def check_file_exists():
    """Проверяет наличие файла и возвращает следующий task_id"""
    file_path =  os.path.expanduser('~/datasets_from_net/for_datalens.csv')
    
    if os.path.exists(file_path):
        print(f"Файл {file_path} существует, переходим к обработке")
        return "run_python_code" # Возвращаем ОДНУ следующую задачу
    else:
        print(f"Файл {file_path} не существует, нужно скачать")
        return "wget_file"  # Возвращаем ОДНУ следующую задачу

def my_python_function():
    import pandas as pd
    df = pd.read_csv('~/datasets_from_net/for_datalens.csv') #pd.read_csv(os.path.expanduser('~/datasets_from_net/for_datalens.csv'))
    df = df.loc[df['Country_store']!='Deutschland']
    df.to_csv('~/datasets_from_net/for_datalens_processed.csv')#df.to_csv(os.path.expanduser('~/datasets_from_net/for_datalens_processed.csv'))

# Определение DAG
default_args = {
    'owner': 'gornostaev',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="simple_sql_python_bash_dag_with_conditioning_new_3",
    default_args=default_args,
    start_date=datetime(2025, 10, 21),
    schedule_interval=None,
    catchup=False,
    tags=["the_only_correct_dag_simple_sql_python_bash_dag_with_conditioning"],
) as dag:
    
    # Task для проверки наличия файла
    check_file_task = BranchPythonOperator(
        task_id="check_file_exists",
        python_callable=check_file_exists,
    )

    # Task для скачивания файла (если его нет)
    download_task = BashOperator(
        task_id="wget_file",
        bash_command="mkdir -p ~/datasets_from_net && wget -P ~/datasets_from_net http://0.0.0.0:7000/for_datalens.csv",
    )

    # Task для обработки данных Python
    python_task = PythonOperator(
        task_id="run_python_code",
        python_callable=my_python_function,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Task для создания таблицы в DuckDB
    create_auto_table = SQLExecuteQueryOperator(
        task_id='create_auto_table',
        conn_id='duck_fashion',
        sql="""
            CREATE OR REPLACE TABLE analytics_data AS
            SELECT *
            FROM read_csv('~/datasets_from_net/for_datalens_processed.csv');
        """,
         trigger_rule=TriggerRule.ONE_SUCCESS,
    )


    #check_file_task >>download_task >> python_task >> create_auto_table
    #check_file_task >> python_task >> create_auto_table

    # Определение зависимостей
    check_file_task >> [download_task, python_task]  # Обе ветви идут от check_file_task
    
    # Если файл нужно скачать: download_task -> python_task -> create_auto_table
    download_task >> python_task #>> create_auto_table
    
    # Если файл уже есть: python_task -> create_auto_table
    python_task >> create_auto_table

    # !!!!!!!!!!!!!!!ЕДИНСТВЕННО ВЕРНЫЙ!!!!!!!!!!!!!!!!!!!!!