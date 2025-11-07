from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


from datetime import datetime, timedelta, date
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


import os
from airflow.sensors.filesystem import FileSensor


latest = Dataset(os.path.expanduser('~/datasets_from_excel/processed/datalens_latest.csv')) #датасет должен быть определен и в родительском и зависимом даге

BASE_DIR = os.path.expanduser('~/datasets_from_excel')

def convert_from_excel_to_csv():
    import pandas as pd
    from datetime import date


    if not pd.read_excel(os.path.expanduser('~/datasets_from_excel/datalens_excel.xlsx'), nrows=5).empty:

        df1=pd.read_excel(os.path.expanduser('~/datasets_from_excel/datalens_excel.xlsx'), nrows=10000)

        td=str(date.today())

        df1.to_csv(os.path.expanduser(f'~/datasets_from_excel/processed/datalens_{td}.csv'), index=False)

        df1.to_csv(os.path.expanduser('~/datasets_from_excel/processed/datalens_latest.csv'), index=False)



        with open(os.path.expanduser(f'~/datasets_from_excel/processed/datalens.csv'), 'r') as file:
            first_line = file.readline().strip()

        if first_line=="":
            df1.to_csv(os.path.expanduser(f'~/datasets_from_excel/processed/datalens.csv'), index=False)
        
        else:
            df1.to_csv(os.path.expanduser(f'~/datasets_from_excel/processed/datalens.csv'),  mode='a', header=False, index=False)




def unpause_dag(**context):
    from airflow.models import DagModel
    from airflow.utils.session import provide_session

    @provide_session
    def _unpause(session=None):
        dag_model = session.query(DagModel).filter(DagModel.dag_id == 'from_dataset_trigger_to_sql').first()
        if dag_model:
            dag_model.is_paused = False
        session.commit()

    _unpause()


# Определение DAG
default_args = {
    'owner': 'gornostaev',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="from_excel_to_csv",
    default_args=default_args,
    start_date=datetime(2025, 10, 21),
    schedule_interval=None, #с этой строкой будет производиться запуск только при нажатии  кнопки "▶"
    #schedule_interval="*/5 * * * *",  # важнейшая часть, пытается запуститься каждые 5 минут - ручной запуск не нужен
    catchup=False,
    tags=["filesensors", "datasets"],
) as dag:
    
    wait_for_file = FileSensor(
    task_id="wait_for_file",
    filepath=os.path.expanduser('~/datasets_from_excel/datalens_excel.xlsx'),
    poke_interval=30,
    timeout=60 * 60,     # 1 час
    mode="reschedule",   # не держим слот
)


    # Task для обработки данных Python
    convert_from_excel_to_csv_ = PythonOperator(
        task_id="convert_from_excel_to_csv_",
        python_callable=convert_from_excel_to_csv,
        outlets=[latest],
    )

    # Task для создания статистики
    renaming = BashOperator(
        task_id='renaming',
        
        bash_command=f'mv "{BASE_DIR}/datalens_excel.xlsx" "{BASE_DIR}/datalens_excel_was_processed_$(date +%Y%m%d).xlsx"',
    )

    unpause = PythonOperator(
    task_id='unpause_dag',
    python_callable=unpause_dag,
)

    trigger_dependent_dag = TriggerDagRunOperator(
        task_id="trigger_dependent_dag",
        trigger_dag_id="from_dataset_trigger_to_sql",
        
    )

    


    wait_for_file >> convert_from_excel_to_csv_ >> renaming >> unpause >> trigger_dependent_dag