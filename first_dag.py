from airflow import DAG
from airflow.operators.python import PythonOperator
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
#from airflow.providers.duckdb.hooks.duckdb import DuckDBHook
from datetime import datetime, timedelta
import pandas as pd

def extract_duckdb_to_csv():
    """
    Получает 5 последних строк из DuckDB через connection и сохраняет в CSV
    """
    # Используем твой connection 'duck_fashion'
    hook = DuckDBHook(duckdb_conn_id='duck_fashion')
    conn = hook.get_conn()
    
    # Выполняем запрос - 5 последних строк
    query = """
    SELECT * 
    FROM transactions_aggregated 
    
    LIMIT 5
    """
    
    df = conn.execute(query).fetchdf()
    
    # Сохраняем в CSV
    #csv_path = "/home/sergigornostaev/airflow/latest_transactions.csv"
    csv_path = "./airflow/latest_transactions_2.csv"
    
    df.to_csv(csv_path, index=False)
    
    print(f"Успешно сохранено {len(df)} строк в {csv_path}")
    return csv_path

# Определение DAG
default_args = {
    'owner': 'gornostaev',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'duckdb_to_csv',
    default_args=default_args,
    description='Извлекает данные из DuckDB в CSV через connection',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['duckdb', 'csv'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_last_5_rows',
        python_callable=extract_duckdb_to_csv,
    )
