from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_modis_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"Iniciando validación de {len(df)} registros")

    valid_mask = (
        (df['frp'] > 0) &
        (df['confidence'] > 30) &
        (df['latitude'].notna()) &
        (df['longitude'].notna()) &
        (df['acq_date'].notna())
    )

    df_valid = df[valid_mask].copy()
    df_rejected = df[~valid_mask].copy()

    df_valid['ingested_at'] = datetime.now()
    df_valid['validation_status'] = 'valid'
    df_valid['source_file'] = 'modis-fire-two-months.csv'
    df_valid['processing_date'] = datetime.now().date()

    df_valid['data_quality_score'] = np.where(
        df_valid['confidence'] >= 80, 100,
        np.where(df_valid['confidence'] >= 50, 75, 50)
    )

    df_rejected['ingested_at'] = datetime.now()
    df_rejected['validation_status'] = 'rejected'
    df_rejected['source_file'] = 'modis-fire-two-months.csv'
    df_rejected['processing_date'] = datetime.now().date()
    df_rejected['data_quality_score'] = 0

    logger.info("Validación completada:")
    logger.info(f"  - Registros válidos: {len(df_valid)}")
    logger.info(f"  - Registros rechazados: {len(df_rejected)}")

    if len(df_rejected) > 0:
        rejection_summary = df_rejected.groupby('validation_status').size().reset_index(name='count')
        logger.info("Motivos de rechazo:")
        for _, row in rejection_summary.iterrows():
            logger.info(f"  - {row['validation_status']}: {row['count']} registros")

    return df_valid, df_rejected

def extract_data() -> str:
    csv_path = 'include/raw/modis-fire-two-months.csv'
    df = pd.read_csv(csv_path)
    temp_path = '/tmp/modis_data.csv'
    df.to_csv(temp_path, index=False)
    logger.info(f"Extraídos {len(df)} registros y guardados temporalmente")
    return temp_path

def validate_and_clean(**context) -> str:
    temp_path = context['ti'].xcom_pull(task_ids='extract_data_raw')
    df = pd.read_csv(temp_path)

    df_valid, df_rejected = validate_modis_data(df)

    valid_path = '/tmp/modis_valid.csv'
    df_valid.to_csv(valid_path, index=False)

    if len(df_rejected) > 0:
        rejected_path = '/tmp/modis_rejected.csv'
        df_rejected.to_csv(rejected_path, index=False)
        logger.info(f"Datos rechazados guardados en: {rejected_path}")

    logger.info(f"Datos válidos listos: {len(df_valid)} registros")
    return valid_path

def create_table_in_public() -> None:
    conn = BaseHook.get_connection('postgres_default')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    with engine.connect() as connection:
        try:
            with connection.begin():
                create_table_sql = text("""
                    CREATE TABLE IF NOT EXISTS fires_bronze (
                        latitude FLOAT,
                        longitude FLOAT,
                        brightness FLOAT,
                        scan FLOAT,
                        track FLOAT,
                        acq_date DATE,
                        acq_time VARCHAR(10),
                        satellite VARCHAR(10),
                        instrument VARCHAR(10),
                        confidence INTEGER,
                        version VARCHAR(10),
                        bright_t31 FLOAT,
                        frp FLOAT,
                        daynight VARCHAR(5),
                        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        validation_status VARCHAR(20) DEFAULT 'valid',
                        source_file VARCHAR(100),
                        processing_date DATE DEFAULT CURRENT_DATE,
                        data_quality_score INTEGER,
                        PRIMARY KEY (acq_date, latitude, longitude, acq_time)
                    )
                """)

                connection.execute(create_table_sql)
                logger.info("Tabla 'fires_bronze' creada/verificada en esquema public")

        except Exception as e:
            logger.error(f"Error creando tabla: {e}")
            raise

def load_bronze_data(**context) -> None:
    valid_path = context['ti'].xcom_pull(task_ids='validate_and_clean')
    df = pd.read_csv(valid_path)

    df['acq_time'] = df['acq_time'].astype(str)
    df['satellite'] = df['satellite'].astype(str)
    df['instrument'] = df['instrument'].astype(str)
    df['version'] = df['version'].astype(str)
    df['daynight'] = df['daynight'].astype(str)

    conn = BaseHook.get_connection('postgres_default')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    try:
        df.to_sql('fires_bronze', engine, if_exists='replace', index=False, method='multi')
        logger.info(f"Datos Bronze cargados exitosamente: {len(df)} registros")

        os.remove(valid_path)

    except Exception as e:
        logger.error(f"Error cargando datos Bronze: {e}")
        raise
    
def load_data(**context) -> None:
    temp_path = context['ti'].xcom_pull(task_ids='extract_data_raw')
    df = pd.read_csv(temp_path)

    df['acq_time'] = df['acq_time'].astype(str)
    df['satellite'] = df['satellite'].astype(str)
    df['instrument'] = df['instrument'].astype(str)
    df['version'] = df['version'].astype(str)
    df['daynight'] = df['daynight'].astype(str)

    conn = BaseHook.get_connection('postgres_default')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df.to_sql('fires_raw', engine, if_exists='replace', index=False)
    os.remove(temp_path)
    logger.info(f"Datos raw cargados exitosamente: {len(df)} registros en PostgreSQL")
    
with DAG(
    dag_id='complete_etl_pipeline',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['modis', 'fire', 'etl', 'dbt', 'bronze'],
    description='ETL pipeline completo con Bronze layer mejorado',
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data_raw',
        python_callable=extract_data,
    )

    validate_task = PythonOperator(
        task_id='validate_and_clean',
        python_callable=validate_and_clean,
    )

    create_table_task = PythonOperator(
        task_id='create_table_in_public',
        python_callable=create_table_in_public,
    )

    load_bronze_task = PythonOperator(
        task_id='load_bronze_data',
        python_callable=load_bronze_data,
    )

    load_raw_task = PythonOperator(
        task_id='load_data_raw_postgres',
        python_callable=load_data,
    )

    dbt_task = BashOperator(
        task_id='transform_with_dbt',
        bash_command='cd /usr/local/airflow/dags/dbt/fire_dbt && dbt run',
    )

    extract_task >> validate_task >> create_table_task >> load_bronze_task >> dbt_task

    extract_task >> load_raw_task