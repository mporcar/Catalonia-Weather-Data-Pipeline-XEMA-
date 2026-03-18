import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from dotenv import load_dotenv

# Load local .env config if running locally or in Codespaces
load_dotenv()

# Read securely from environment. Do not fallback to hardcoded strings!
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME")
BQ_DATASET = os.environ.get("BQ_DATASET", "xema_weather")

# Enforce secure configuration
if not GCP_PROJECT_ID or not GCP_BUCKET_NAME:
    raise ValueError("Missing critical environment variables: GCP_PROJECT_ID and/or GCP_BUCKET_NAME.")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'xema_daily_weather_pipeline',
    default_args=default_args,
    description='Daily ingestion of Catalonia weather data (XEMA)',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['xema', 'weather', 'gcp', 'dbt'],
) as dag:

    # Task 1: Fetch data from XEMA API and upload to GCS Data Lake
    ingest_to_gcs = BashOperator(
        task_id='ingest_data_to_gcs',
        bash_command=f'python /opt/airflow/dags/scripts/ingest_data.py --date {{{{ ds }}}} --bucket {GCP_BUCKET_NAME}',
    )

    # Task 2: Load Parquet from GCS to BigQuery Native Table
    load_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq_staging',
        bucket=GCP_BUCKET_NAME,
        source_objects=[f'raw/weather_data/data_lectura={{{{ ds }}}}/data.parquet'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.raw_weather_data',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        autodetect=True,
        time_partitioning={'type': 'DAY', 'field': 'data_lectura'},
    )

    # Task 3: Run dbt models to transform raw data
    run_dbt_models = BashOperator(
        task_id='run_dbt_transformations',
        bash_command='cd /opt/airflow/dbt_xema && dbt run --profiles-dir . && dbt test --profiles-dir .',
    )

    ingest_to_gcs >> load_to_bq >> run_dbt_models
