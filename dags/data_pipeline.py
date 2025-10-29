from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import os

# Database connection
DB_CONN = 'postgresql://airflow:airflow@postgres:5432/airflow'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='ETL Pipeline with parallel processing',
    schedule_interval='@daily',
    catchup=False,
)

# TODO: Implement your functions here
def ingest_dataset1(**context):
    """Ingest first dataset"""
    df = pd.read_csv('/opt/airflow/data/dataset1.csv')
    output_path = '/tmp/dataset1_raw.csv'
    df.to_csv(output_path, index=False)
    context['ti'].xcom_push(key='dataset1_path', value=output_path)
    print(f"Dataset 1 ingested: {len(df)} rows")

def ingest_dataset2(**context):
    """Ingest second dataset"""
    df = pd.read_csv('/opt/airflow/data/dataset2.csv')
    output_path = '/tmp/dataset2_raw.csv'
    df.to_csv(output_path, index=False)
    context['ti'].xcom_push(key='dataset2_path', value=output_path)
    print(f"Dataset 2 ingested: {len(df)} rows")

# TODO: Add more functions for transform, merge, load, analyze, cleanup

# Define tasks
task_ingest1 = PythonOperator(
    task_id='ingest_dataset1',
    python_callable=ingest_dataset1,
    dag=dag,
)

task_ingest2 = PythonOperator(
    task_id='ingest_dataset2',
    python_callable=ingest_dataset2,
    dag=dag,
)

# TODO: Add more tasks and dependencies
# Example: [task_ingest1, task_ingest2] >> task_merge