from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import datetime, timedelta, date


def print_context_vars(**kwargs):
    ds = kwargs['ds']
    data_interval_start = kwargs['data_interval_start']
    data_interval_end = kwargs['data_interval_end']
    
    print(f"ds: {ds}")
    print(f"data_interval_start: {data_interval_start}")
    print(f"data_interval_end: {data_interval_end}")
    
    return [ds, str(data_interval_start), str(data_interval_end), kwargs['logical_date'].strftime('%Y-%m-%d')]




default_args = {
    'owner': 'gornostaev',
    'depends_on_past': False,
    
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="date_context_weekly",
    start_date=datetime(2025, 10, 1),
    schedule="@weekly",
    catchup=True,
    default_args=default_args,
) as dag:
    
    task1 = PythonOperator(
        task_id="print_context",
        python_callable=print_context_vars,
    )

