from airflow import DAG
from airflow.operators.python import PythonOperator
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from datetime import datetime, timedelta
import pandas as pd
import os
from pathlib import Path

def extract_duckdb_to_csv():
    """
    Получает 5 последних строк из DuckDB и сохраняет в CSV
    + сохраняет диагностику путей и окружения в path_searching.txt
    """
    hook = DuckDBHook(duckdb_conn_id='duck_fashion')
    conn = hook.get_conn()

    query = """
    SELECT * 
    FROM transactions_aggregated 
    LIMIT 5
    """

    df = conn.execute(query).fetchdf()

    # --- Формируем рабочую директорию ---
    output_dir = Path.home() / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "latest_transactions.csv"
    info_path = output_dir / "path_searching.txt"

    # --- Собираем отладочную информацию ---
    lines = []
    lines.append(f"datetime: {datetime.now()}")
    lines.append(f"os.getcwd(): {os.getcwd()}")
    lines.append(f"Path.home(): {Path.home()}")
    lines.append(f"AIRFLOW_HOME: {os.getenv('AIRFLOW_HOME')}")
    lines.append(f"USER: {os.getenv('USER')}")
    lines.append(f"LOGNAME: {os.getenv('LOGNAME')}")
    lines.append(f"uid/gid: {os.getuid()}/{os.getgid()}")
    lines.append("Проверка путей:")
    for p in [
        "/latest_transactions.csv",
        "/airflow/latest_transactions.csv",
        "/home/sergigornostaev/airflow/latest_transactions.csv",
        str(output_dir / 'latest_transactions.csv')
    ]:
        writable = os.access(os.path.dirname(p) or "/", os.W_OK)
        lines.append(f"{p} — exists: {os.path.exists(p)} writable: {writable}")

    # --- Пишем диагностику ---
    with open(info_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # --- Сохраняем CSV ---
    df.to_csv(csv_path, index=False)

    print(f"Успешно сохранено {len(df)} строк в {csv_path}")
    print(f"Диагностика путей сохранена в {info_path}")

    return str(csv_path)

# --- Определение DAG ---
default_args = {
    'owner': 'gornostaev',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'duckdb_to_csv_3',
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