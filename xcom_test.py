from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

def process_data(**context):
    """Первая задача - обрабатывает данные и возвращает статистику"""
    import pandas as pd
    
    # Читаем данные
    file_path = os.path.expanduser('~/datasets_from_net/for_datalens.csv')
    df = pd.read_csv(file_path)
    
    # Фильтруем
    df = df.loc[df['Country_store'] != 'Deutschland']
    
    # Получаем статистику
    stats = df.describe()
    
    # Преобразуем в словарь для XCom (DataFrame плохо сериализуется)
    stats_dict = stats.to_dict()
    
    print(f"Обработано {len(df)} строк. Статистика:")
    print(stats)
    
    # Возвращаем данные через return (автоматически в XCom)
    return stats_dict

def save_statistics_to_csv(**context):
    """Вторая задача - получает статистику из XCom и сохраняет в CSV"""
    import pandas as pd
    
    # Получаем данные из XCom предыдущей задачи
    ti = context['ti']
    stats_dict = ti.xcom_pull(task_ids='run_python_code')  # ⚡ Ключевой момент!
    
    # Преобразуем обратно в DataFrame
    stats_df = pd.DataFrame(stats_dict)
    
    # Сохраняем в файл
    output_path = os.path.expanduser('~/datasets_from_net/for_datalens_stats.csv')
    stats_df.to_csv(output_path)
    
    print(f"Статистика сохранена в: {output_path}")
    print("Содержимое статистики:")
    print(stats_df)

# Определение DAG
default_args = {
    'owner': 'gornostaev',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="xcom_test",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["example"],
) as dag:
    
    # Первая задача - обработка данных
    process_task = PythonOperator(
        task_id="run_python_code",
        python_callable=process_data,
        provide_context=True,  # ⚡ Важно для доступа к context
    )

    # Вторая задача - сохранение статистики
    save_stats_task = PythonOperator(
        task_id='save_statistics',
        python_callable=save_statistics_to_csv,
        provide_context=True,  # ⚡ Важно для доступа к XCom
    )

    process_task >> save_stats_task