from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Salesdata_dag',
    default_args=default_args,
    description='A DAG to read, transform, and load CSV data',
    schedule_interval='0 9 * * *',  # Set to 'daily' or any other interval as needed
)


def read_excel(**kwargs):
    file_path = r'sample_data/Sample - Superstore.xls'
    df = pd.read_excel(file_path)
    # Serialize DataFrame to JSON for XCom
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())


def transform_data(**kwargs):
    ti = kwargs['ti']
    # Pull and deserialize DataFrame from XCom
    raw_data = ti.xcom_pull(key='raw_data', task_ids='read_excel')
    df = pd.read_json(raw_data)

    # Perform transformation
    df['transformed_column'] = df['Sales'] / df['Quantity']
    # Serialize transformed DataFrame to JSON for XCom
    ti.xcom_push(key='transformed_data', value=df.to_json())


def load_to_csv(**kwargs):
    ti = kwargs['ti']
    # Pull and deserialize DataFrame from XCom
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_data')
    df = pd.read_json(transformed_data)

    output_csv_file_path = r'output_data/Sales_DAG.csv'
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_csv_file_path), exist_ok=True)
    df.to_csv(output_csv_file_path, index=False)


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

# Set task dependencies
read_excel_task >> transform_data_task >> load_to_csv_task

