import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ETL_PROCESS.ETL import extract_json, extract_sql, extract_xml, transform_data, load_data, engine

# Аргументы по умолчанию для каждой задачи в DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 18),
    'email': ['shabashev.va@yandex.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Инициализация DAG
dag = DAG(
    'test_ETL',
    default_args=default_args,
    description='Тестирование использования airflow для ETL',
    schedule='@daily',
)
   
# Задачи для ETL процесса
def extract_task(**kwargs):
    json_df = extract_json('ETL_PROCESS/data/data.json')
    sql_df = extract_sql(engine, "SELECT id, name, price, quantity, category FROM public.products;")
    xml_df = extract_xml('ETL_PROCESS/data/data.xml')
    # Сохранение данных для последующего использования в других задачах
    kwargs['ti'].xcom_push(key='json_df', value=json_df.to_json(orient='split'))
    kwargs['ti'].xcom_push(key='sql_df', value=sql_df.to_json(orient='split'))
    kwargs['ti'].xcom_push(key='xml_df', value=xml_df.to_json(orient='split'))

def transform_task(**kwargs):
    ti = kwargs['ti']
    # Извлечение данных из предыдущей задачи
    json_df = pd.read_json(ti.xcom_pull(task_ids='extract', key='json_df'), orient='split')
    sql_df = pd.read_json(ti.xcom_pull(task_ids='extract', key='sql_df'), orient='split')
    xml_df = pd.read_json(ti.xcom_pull(task_ids='extract', key='xml_df'), orient='split')
    # Преобразование и объединение данных
    combined_df = pd.concat([json_df, sql_df, xml_df], ignore_index=True)
    transformed_df = transform_data(combined_df)
    # Сохранение преобразованных данных для последующего использования
    ti.xcom_push(key='transformed_df', value=transformed_df.to_json(orient='split'))

def load_task(**kwargs):
    ti = kwargs['ti']
    # Извлечение преобразованных данных из предыдущей задачи
    transformed_df = pd.read_json(ti.xcom_pull(task_ids='transform', key='transformed_df'), orient='split')
    # Загрузка данных в базу данных
    load_data(transformed_df, engine, 'data_load')

# Определение задач с использованием PythonOperator
extract_operator = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    dag=dag,
)

transform_operator = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    dag=dag,
)

load_operator = PythonOperator(
    task_id='load',
    python_callable=load_task,
    dag=dag,
)

# Настройка последовательности выполнения задач
extract_operator >> transform_operator >> load_operator
