from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook

def extract_data():
    csv_path = 'include/raw/modis-fire-two-months.csv'
    df = pd.read_csv(csv_path)
    temp_path = '/tmp/modis_data.csv'
    df.to_csv(temp_path, index=False)
    print(f"Extraídos {len(df)} registros y guardados temporalmente")
    return temp_path

def load_data(**context):
    temp_path = context['ti'].xcom_pull(task_ids='extract_data_raw')
    df = pd.read_csv(temp_path)
    conn = BaseHook.get_connection('postgres_default')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df.to_sql('fires_raw', engine, if_exists='replace', index=False)
    os.remove(temp_path)
    print(f"Datos cargados exitosamente: {len(df)} registros en PostgreSQL")

with DAG(
    dag_id='complete_etl_pipeline',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['modis', 'fire', 'etl', 'dbt'],
) as dag:
    extract_task = PythonOperator(
        task_id='extract_data_raw',
        python_callable=extract_data,
    )
    
    load_task = PythonOperator(
        task_id='load_data_raw_postgres',
        python_callable=load_data,
    )
    
    dbt_task = BashOperator(
        task_id='transform_with_dbt',
        bash_command='cd /usr/local/airflow/dags/dbt/fire_dbt && dbt run',
    )
    
    extract_task >> load_task >> dbt_task