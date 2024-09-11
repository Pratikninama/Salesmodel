from airflow import DAG
from airflow.operators.python import PythonOperator
import xlrd
import pandas as pd
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'read_transform_load_csv',
    default_args=default_args,
    description='A DAG to read, transform, and load CSV data',
    schedule='0 9 * * *',  # Set to 'daily' or any other interval as needed
)


def read_excel(**kwargs):
    excel_file_path = pd.read_excel(r'sample_data\Sample - Superstore.xls')
    df = pd.read_excel(excel_file_path, **kwargs)
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_dict())


def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(key='raw_data', task_ids='read_excel')
    df = pd.DataFrame.from_dict(raw_data)

    df['transformed_column'] = df['Sales'] /df['Quantity']
    ti.xcom_push(key='transformed_data', value=df.to_dict())

def load_to_csv(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_data')
    df = pd.DataFrame.from_dict(transformed_data)
    output_csv_file_path = r'output_data/Sales_DAG.csv'
    df.to_csv('output_data/Sales_DAG.csv', index=False)


# Define tasks
read_excel_task = PythonOperator(
    task_id='read_excel',
    python_callable=read_excel,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_to_csv_task = PythonOperator(
    task_id='load_to_csv',
    python_callable=load_to_csv,
    provide_context=True,
    dag=dag,
)

read_excel_task >> transform_data_task >> load_to_csv_task
#impport sales
