import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from dotenv import load_dotenv

# Dynamically determine paths so it works in Codespaces without hardcoded /opt/airflow
DAGS_FOLDER  = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAGS_FOLDER)

# Load local .env config to ensure credentials and IDs are populated
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

# Read securely from environment. Do not fallback to hardcoded strings!
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME")
BQ_DATASET = os.environ.get("BQ_DATASET", "xema_weather")

# Enforce secure configuration
if not GCP_PROJECT_ID or not GCP_BUCKET_NAME:
    raise ValueError(
        "Missing critical environment variables: GCP_PROJECT_ID and/or GCP_BUCKET_NAME. "
        "Please ensure your .env file is loaded."
    )

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
    description='Daily ingestion of Catalonia weather data (XEMA) with dimension enrichment',
    schedule_interval='@daily',
    start_date=datetime(2026, 3, 1),
    catchup=True,
    tags=['xema', 'weather', 'gcp', 'dbt'],
    max_active_runs=3   # at most 3 days running simultaneously
) as dag:

    # ------------------------------------------------------------------
    # Task 1 — Fetch weather observations + both dimension tables → GCS
    # ------------------------------------------------------------------
    ingest_to_gcs = BashOperator(
        task_id='ingest_data_to_gcs',
        bash_command=(
            f'python {PROJECT_ROOT}/dags/scripts/ingest_data.py '
            f'--date {{{{ ds }}}} '
            f'--bucket {GCP_BUCKET_NAME}'
        ),
    )

    # ------------------------------------------------------------------
    # Task 2a — Load weather Parquet → BQ (append, partitioned by date)
    # ------------------------------------------------------------------
    load_weather_to_bq = GCSToBigQueryOperator(
    task_id='load_gcs_to_bq_staging',
    bucket=GCP_BUCKET_NAME,
    source_objects=['raw/weather_data/{{ ds }}/data.parquet'],
    destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.raw_weather_data${{{{ ds_nodash }}}}',
    source_format='PARQUET',
    write_disposition='WRITE_TRUNCATE',     # ← replaces only that day's partition
    autodetect=True,
    time_partitioning={'type': 'DAY', 'field': 'data_lectura'},
)

    # ------------------------------------------------------------------
    # Task 2b — Load variables dimension → BQ (full truncate each day)
    # ------------------------------------------------------------------
    load_dim_variables_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq_dim_variables',
        bucket=GCP_BUCKET_NAME,
        source_objects=['raw/dim_variables/latest/data.parquet'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.raw_dim_variables',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',   # full-refresh: small reference table
        autodetect=True,
    )

    # ------------------------------------------------------------------
    # Task 2c — Load stations dimension → BQ (full truncate each day)
    # ------------------------------------------------------------------
    load_dim_stations_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq_dim_stations',
        bucket=GCP_BUCKET_NAME,
        source_objects=['raw/dim_stations/latest/data.parquet'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.raw_dim_stations',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',   # full-refresh: small reference table
        autodetect=True,
    )

    # ------------------------------------------------------------------
    # Task 3 — dbt: transform raw → staging → mart
    # All three BQ loads must finish before dbt reads from them.
    # ------------------------------------------------------------------
    run_dbt_models = BashOperator(
        task_id='run_dbt_transformations',
        bash_command=(
            f'cd {PROJECT_ROOT}/dbt_xema '
            f'&& dbt run --profiles-dir . '
            f'&& dbt test --profiles-dir .'
        ),
    )

    # ------------------------------------------------------------------
    # Dependency graph
    #
    #   ingest_to_gcs
    #       ├── load_weather_to_bq ──────┐
    #       ├── load_dim_variables_to_bq ├──► run_dbt_models
    #       └── load_dim_stations_to_bq ─┘
    # ------------------------------------------------------------------
    ingest_to_gcs >> [load_weather_to_bq, load_dim_variables_to_bq, load_dim_stations_to_bq]

    [load_weather_to_bq, load_dim_variables_to_bq, load_dim_stations_to_bq] >> run_dbt_models