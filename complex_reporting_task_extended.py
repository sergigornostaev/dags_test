from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta, date
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import os
from airflow.sensors.filesystem import FileSensor
import io

### в данном примере есть скачивание по прямой ссылке через bash, через интернет не по прямой ссылке, выгрузка данных из БД, проверка состояния БД,
### pandas операции, запись данных в duckdb

BASE_DIR = os.path.expanduser('~/aggregate_reporting')

base_date='2025-03-01'

bash_command = rf"""
    filename=$(wget -q -O - http://0.0.0.0:7000/ | grep -oE '[^"]*\.csv' | grep "{base_date}" | grep "transactions" | head -1)
    if [[ -n "$filename" ]]; then
        wget -P $BASE_DIR/archive "http://0.0.0.0:7000/$filename" && \
        mv "$BASE_DIR/archive/$filename" "$BASE_DIR/archive/raw_$filename"
        echo "/archive/raw_$filename"
    else
        echo "File with patterns '{base_date}' and 'transactions' not found"
        exit 1
    fi
""" 
#два способа передать переменные в bash команду. f строки и переменные внутри bash


def reading_receiving_ids(ti):
    xcom_value = ti.xcom_pull(task_ids="from_local_host", key="return_value")
    file_path = os.path.join(BASE_DIR, xcom_value.lstrip('/'))
    dft = pd.read_csv(file_path)
    
    dft['Date'] = dft['Date_time'].astype("datetime64[ns]").dt.date.astype("datetime64[ns]")

    dft['Date_of_append']=pd.to_datetime(base_date) 
    
    stores_list = dft['Store_ID'].unique().astype(str).tolist()
    customers_list = dft['Customer_ID'].unique().astype(str).tolist()
    products_list = dft['Product_ID'].unique().astype(str).tolist()

    POSTGRES_CONN_ID = "fashion_postgre"
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)

    # Безопасные запросы
    def safe_get_df(table_name, id_list, id_column):
        if not id_list:
            return pd.DataFrame()
        placeholders = ','.join(['%s'] * len(id_list))
        return pg_hook.get_pandas_df(
            f'SELECT * FROM {table_name} WHERE {id_column} IN ({placeholders})', 
            parameters=id_list
        )

    dfs = safe_get_df('stores', stores_list, 'store_id')
    dfp = safe_get_df('products', products_list, 'product_id')  
    dfc = safe_get_df('customers', customers_list, 'customer_id')

    # Сохранение файлов
    dft.to_csv(os.path.join(BASE_DIR, f'transactions_for_sql_{base_date}.csv'), index=False)
    
    # Правильный merge
    df_merged = (dft.rename(lambda x: x.lower(), axis='columns')
                 .merge(dfs, on='store_id')
                 .merge(dfp, on='product_id')
                 .merge(dfc, on='customer_id'))
    
    df_merged.to_csv(os.path.join(BASE_DIR, f'merged_{base_date}.csv'), index=False)



    return {"sql": os.path.join(BASE_DIR, f'transactions_for_sql_{base_date}.csv'), 
            "merged": os.path.join(BASE_DIR, f'merged_{base_date}.csv')}


def download_from_net_func():
        csv_url = "https://2111.filemail.com/api/file/get?filekey=9SveaQLpIYfevbCPM7uLGbgJM-c_ZF6H00rmBWjNGE6jv3tYKSa1mdzVCnuWkXI&pk_vid=5618854c7b19ac591761600360167451" #с сайта https://www.filemail.com/
        #!!!!!!!!!!!!!!!!!!---можно и так---!!!!!!!!!!!!!!!!!!
        #pd.read_csv('https://2111.filemail.com/api/file/get?filekey=9SveaQLpIYfevbCPM7uLGbgJM-c_ZF6H00rmBWjNGE6jv3tYKSa1mdzVCnuWkXI&pk_vid=5618854c7b19ac591761600360167451')
        
        # Создаем директорию если не существует
        archive_dir = os.path.join(BASE_DIR, 'archive')
        #os.makedirs(archive_dir, exist_ok=True)
        
        local_filename = os.path.join(archive_dir, f'employees_{base_date}.csv')

        try:
            # Скачиваем файл
            response = requests.get(csv_url, allow_redirects=True)
            response.raise_for_status()

            # Сохраняем в файл
            with open(local_filename, 'wb') as file:
                file.write(response.content)
            
            print(f"CSV file successfully downloaded to {local_filename}")

            # Читаем из сохраненного файла
            df = pd.read_csv(local_filename)

            store_col = df.rename(lambda x: x.lower(), axis='columns').filter(like='store', axis=1).columns[0]
            employee_col = df.rename(lambda x: x.lower(), axis='columns').filter(like='employee', axis=1).columns[0]

            result = df.rename(lambda x: x.lower(), axis='columns').groupby(store_col)[employee_col].count().reset_index().set_axis(['store_id', 'employee_count'], axis=1)

            return result.to_dict()  # или обрабатывай дальше

        except requests.exceptions.RequestException as e:
            print(f"Error downloading the CSV file: {e}")
            
            error_msg = f"Error downloading the CSV file: {e}"
            print(error_msg)
            raise Exception(error_msg) ### !!!!! ОЧЕНЬ ВАЖНО возвращать ошибку, а не return None, чтоб застопорить все последующие downstream таски !!!!!
        

def check_data():
    ddb = DuckDBHook("duck_fashion") #pg = PostgresHook("duck_fashion") если бы был postgreSql
    df = ddb.get_pandas_df(
        f"SELECT COUNT(*) as cnt FROM transaction_copy WHERE Date_of_append = '{base_date}'"
    )

    if df['cnt'][0] > 0:
        return "skip_processing"
    else:
        return "from_local_host"



def another_merge_and_sql_action(ti): #ТУТ САМЫЙ ВЕРНЫЙ ПРИМЕР РАБОТЫ XCom
    hook = DuckDBHook("duck_fashion")
    conn = hook.get_conn()
    table_name = "for_dashboarding_tab"

    # Получаем данные из XCom
    reading_result = ti.xcom_pull(task_ids="reading_file", key="return_value") #при работе с xcom в питоне jinja {{}} не нужны
    path = reading_result["merged"]
    employees_data = ti.xcom_pull(task_ids="download_from_net", key="return_value")
    
    # Читаем основной CSV файл
    df = pd.read_csv(path)
    
    # Преобразуем данные о сотрудниках из словаря в DataFrame
    df_employees = pd.DataFrame(employees_data)
    
    # Выполняем merge с данными о сотрудниках
    df_merged = df.merge(df_employees, how='left', on='store_id')
    
    try:
        # Пытаемся создать таблицу - DuckDB автоматически зарегистрирует df как таблицу
        create_table_sql = f"""
        CREATE TABLE {table_name} AS 
        SELECT * FROM df_merged
        """
        conn.execute(create_table_sql)
        print(f"Table {table_name} created successfully")
        
    except Exception as e:
        # Проверяем, связано ли исключение с тем, что таблица уже существует
        error_msg = str(e).lower()
        if "already exists" in error_msg or "table" in error_msg and "exists" in error_msg:
            print(f"Table {table_name} already exists, checking for existing data...")
            
            # Проверяем, есть ли уже данные за эту дату
            check_sql = f"SELECT COUNT(*) as cnt FROM {table_name} WHERE Date_of_append = '{base_date}'"
            result = conn.execute(check_sql).df()
            
            if result['cnt'][0] == 0:
                # Добавляем данные, если их еще нет
                insert_sql = f"INSERT INTO {table_name} SELECT * FROM df_merged"
                conn.execute(insert_sql)
                print(f"Data for {base_date} inserted into {table_name}")
            else:
                print(f"Data for {base_date} already exists in {table_name}")
        else:
            # Если это другое исключение - пробрасываем его дальше
            print(f"Unexpected error: {e}")
            raise
    
    # Закрываем соединение
    conn.close()
    
    return f"Data processed for table {table_name}"



# Определение DAG
default_args = {
    'owner': 'gornostaev',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="complex_reporting_task_extended",
    default_args=default_args,
    start_date=datetime(2025, 10, 21),
    schedule_interval=None, #с этой строкой будет производиться запуск только при нажатии  кнопки "▶"
    #schedule_interval="*/5 * * * *",  # важнейшая часть, пытается запуститься каждые 5 минут - ручной запуск не нужен
    catchup=False,
    tags=["postgre", "duckdb", 'combination_of_2_dbs'],
) as dag:
    

    check_data_exists = BranchPythonOperator(
        task_id="check_data_exists",
        python_callable=check_data,
    )

    skip_processing = EmptyOperator(
        task_id="skip_processing",
        #trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    )

    from_local_host = BashOperator(
        task_id='from_local_host',
        
        bash_command=bash_command,
        env={'BASE_DIR': BASE_DIR}, #так можно переместить питон переменную в bash переменную
        do_xcom_push=True,
    )

    process_task = PythonOperator(
        task_id="reading_file",
        python_callable=reading_receiving_ids,
        do_xcom_push=True,
    )

    download_from_net = PythonOperator(
        task_id="download_from_net",
        python_callable=download_from_net_func,
        do_xcom_push=True,
    )

    sql_inserting = SQLExecuteQueryOperator(
        task_id='sql_inserting',
        conn_id='duck_fashion',
        sql="""
            INSERT INTO transaction_copy 
            SELECT * 
            FROM read_csv('{{ti.xcom_pull(task_ids="reading_file", key="return_value")['sql']}}');
        """,
         trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    ###    sql="""
     ###   COPY transaction_copy 
    ###    FROM '{{ti.xcom_pull(task_ids="reading_file", key="return_value")["sql"]}}'
    ###    WITH (FORMAT csv, HEADER true);
    ###""" - так можно грузить данные в postgre


    create_dashboarding_table=PythonOperator(
        task_id="create_dashboarding_table",
        python_callable=another_merge_and_sql_action,
        
    )

    check_data_exists >> [skip_processing, from_local_host]

    from_local_host >> process_task >> sql_inserting

    from_local_host >> download_from_net >> create_dashboarding_table

