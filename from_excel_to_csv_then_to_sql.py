from airflow import DAG, Dataset

from airflow.operators.python import PythonOperator
from airflow.api.client.local_client import Client
from datetime import datetime, timedelta, date
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os

latest = Dataset(os.path.expanduser('~/datasets_from_excel/processed/datalens_latest.csv'))#датасет должен быть определен и в родительском и зависимом даге



def cleaning():
    import pandas as pd
    td=str(date.today())

    df = pd.read_csv(os.path.expanduser('~/datasets_from_excel/processed/datalens_latest.csv'))
    df = df.dropna().drop_duplicates()
    df['date_of_adding']=pd.to_datetime(td)
    path=os.path.expanduser(f'~/datasets_from_excel/processed/datalens_latest_for_sql_{td}.csv')
    df.to_csv(path)
    return path


def generate_sql(**context):
    # Получаем путь к файлу из предыдущей задачи
    file_path = context['ti'].xcom_pull(task_ids='cleaning_')
    
    # Генерируем SQL с явным указанием колонок
    sql = f"""

        ALTER TABLE analytics_data 
ADD COLUMN IF NOT EXISTS date_of_adding TIMESTAMP;

-- Вставляем данные
INSERT INTO analytics_data 
SELECT * FROM read_csv('{file_path}');
    """
    
    return sql


def pause_dags(**context):
    from airflow.models import DagModel
    from airflow.utils.session import provide_session

    @provide_session
    def _pause(session=None):
        dags = ['from_excel_to_csv', 'from_dataset_trigger_to_sql']
        for dag_id in dags:
            dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            if dag_model:
                dag_model.is_paused = True
        session.commit()

    _pause()

# Определение DAG
default_args = {
    'owner': 'gornostaev',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="from_dataset_trigger_to_sql",
    default_args=default_args,
    start_date=datetime(2025, 10, 21),
    schedule=None, #!!!!!![latest],
    
    catchup=False,
    tags=["filesensors", "datasets"],
) as dag:
    
 


    # Task для обработки данных Python
    cleaning_ = PythonOperator(
        task_id="cleaning_",
        python_callable=cleaning,
    )

    generate_sql_ = PythonOperator(
        task_id="generate_sql",
        python_callable=generate_sql,
    )

    # Task для создания таблицы в DuckDB
    append_data_to_sql = SQLExecuteQueryOperator(
        task_id='append_data_to_sql',
        conn_id='duck_fashion',
        sql="{{ ti.xcom_pull(task_ids='generate_sql') }}"
    )

    pause_task = PythonOperator( task_id='pause_dags', python_callable=pause_dags ) 
    
    
    cleaning_ >> generate_sql_ >> append_data_to_sql >> pause_task



    