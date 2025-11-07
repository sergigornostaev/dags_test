# %%
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import os

# --- Python-функции ---
def check_file_exists():
    """Проверяет наличие файла и возвращает task_id для следующего шага"""
    file_path = os.path.expanduser('~/datasets_from_net/for_datalens.csv')
    
    if os.path.exists(file_path):
        print(f"✅ Файл {file_path} существует — переходим к обработке")
        return "run_python_code"
    else:
        print(f"❌ Файл {file_path} не найден — нужно скачать")
        return "wget_file"


def my_python_function():
    """Пример обработки данных"""
    import pandas as pd
    df = pd.read_csv(os.path.expanduser('~/datasets_from_net/for_datalens.csv'))
    df = df.loc[df['Country_store'] != 'Deutschland']
    df.to_csv(os.path.expanduser('~/datasets_from_net/for_datalens_processed.csv'), index=False)
    print("✅ Файл обработан и сохранён")


# --- Аргументы по умолчанию ---
default_args = {
    'owner': 'gornostaev',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


# --- Определение DAG ---
with DAG(
    dag_id="simple_sql_python_bash_dag_with_conditioning_new",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["example"],
) as dag:

    # 1️⃣ Проверяем наличие файла
    check_file_task = BranchPythonOperator(
        task_id="check_file_exists",
        python_callable=check_file_exists,
    )

    # 2️⃣ Скачиваем файл (если его нет)
    download_task = BashOperator(
        task_id="wget_file",
        bash_command=(
            "mkdir -p ~/datasets_from_net && "
            "wget -q -P ~/datasets_from_net http://0.0.0.0:7000/for_datalens.csv"
        ),
    )

    # 3️⃣ Обработка данных
    python_task = PythonOperator(
        task_id="run_python_code",
        python_callable=my_python_function,
    )

    # 4️⃣ Соединяем ветки
    join_after_branch = EmptyOperator(
        task_id="join_after_branch",
        trigger_rule="none_failed_min_one_success"
    )

    # 5️⃣ Загружаем в DuckDB
    create_auto_table = SQLExecuteQueryOperator(
        task_id='create_auto_table',
        conn_id='duck_fashion',
        sql="""
            CREATE OR REPLACE TABLE analytics_data AS
            SELECT *
            FROM read_csv('~/datasets_from_net/for_datalens_processed.csv');
        """
    )

    # --- Зависимости ---
    check_file_task >> [download_task, python_task]
    download_task >> python_task
    [download_task, python_task] >> join_after_branch
    join_after_branch >> create_auto_table





