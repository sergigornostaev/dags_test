from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def process_data():
    """Первая функция - обрабатывает данные"""
    import pandas as pd
    file_path = os.path.expanduser('~/datasets_from_net/for_datalens.csv')
    df = pd.read_csv(file_path)
    df = df.loc[df['Country_store'] != 'Deutschland']
    
    # Возвращаем статистику (автоматически идет в XCom)
    return df.describe().to_dict()

def save_statistics(**context):
    """Вторая функция - получает данные через context"""
    import pandas as pd
    
    # Получаем task instance из context
    ti = context['ti']
    
    # Достаем данные из XCom предыдущей задачи
    tab = ti.xcom_pull(task_ids='run_python_code')
    
    # Сохраняем в CSV
    pd.DataFrame(tab).to_csv(os.path.expanduser('~/datasets_from_net/for_datalens_stats.csv'))
    print(f"Статистика сохранена, размер: {len(tab)} показателей")

# Определение DAG
default_args = {
    'owner': 'gornostaev',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
}

with DAG(
    dag_id="xcom_test_1",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["example"],
) as dag:

    # Task для обработки данных
    python_task = PythonOperator(
        task_id="run_python_code",
        python_callable=process_data,
    )

    # Task для создания статистики
    create_stat_text = PythonOperator(
        task_id='create_stat_text',
        python_callable=save_statistics,
        provide_context=True,  # 🔑 Ключевой параметр!
    )

    python_task >> create_stat_text