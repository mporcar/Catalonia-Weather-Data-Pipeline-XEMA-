import argparse
import logging
import os
from datetime import datetime, timedelta
import pandas as pd
import requests
from google.cloud import storage
import io
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

XEMA_BASE_URL = "https://analisi.transparenciacatalunya.cat/resource"

VARIABLES_ENDPOINT   = f"{XEMA_BASE_URL}/4fb2-n3yi.csv"
STATIONS_ENDPOINT    = f"{XEMA_BASE_URL}/yqwd-vj5e.csv"
WEATHER_ENDPOINT     = f"{XEMA_BASE_URL}/nzvn-apee.csv"

STATION_CODES = ('D5', 'X8', 'AN', 'X4', 'X2')


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def fetch_weather_data(target_date_str: str) -> pd.DataFrame:
    """Fetches weather observations from the XEMA API for a specific date."""
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
    next_date   = target_date + timedelta(days=1)

    station_list = ", ".join(f"'{s}'" for s in STATION_CODES)
    where_clause = (
        f"data_lectura >= '{target_date}T00:00:00' "
        f"AND data_lectura < '{next_date}T00:00:00' "
        f"AND codi_estacio IN ({station_list})"
    )

    params = {"$where": where_clause, "$limit": 500_000}

    logger.info(f"Fetching weather observations for date: {target_date}")
    response = requests.get(WEATHER_ENDPOINT, params=params, timeout=60)
    response.raise_for_status()

    df = pd.read_csv(io.StringIO(response.text))
    df['data_lectura'] = pd.to_datetime(df['data_lectura'], utc=True).dt.date
    df['reading_timestamp'] = pd.to_datetime(df['data_lectura'], utc=True)

    logger.info(f"Fetched {len(df)} weather rows.")
    return df


def fetch_dim_variables() -> pd.DataFrame:
    """Fetches the full meteorological variables dimension table."""
    logger.info("Fetching meteorological variables dimension table.")
    response = requests.get(VARIABLES_ENDPOINT, params={"$limit": 10_000}, timeout=60)
    response.raise_for_status()

    df = pd.read_csv(io.StringIO(response.text))
    # Normalise column names to lowercase so downstream SQL is consistent
    df.columns = [c.lower().strip() for c in df.columns]
    logger.info(f"Fetched {len(df)} variable records.")
    return df


def fetch_dim_stations() -> pd.DataFrame:
    """Fetches the full meteorological stations dimension table."""
    logger.info("Fetching meteorological stations dimension table.")
    response = requests.get(STATIONS_ENDPOINT, params={"$limit": 10_000}, timeout=60)
    response.raise_for_status()

    df = pd.read_csv(io.StringIO(response.text))
    df.columns = [c.lower().strip() for c in df.columns]
    logger.info(f"Fetched {len(df)} station records.")
    return df


# ---------------------------------------------------------------------------
# GCS upload helper
# ---------------------------------------------------------------------------

def upload_to_gcs(df: pd.DataFrame, bucket_name: str, destination_blob_name: str) -> None:
    """Uploads a pandas DataFrame to GCS as a Parquet file."""
    if df.empty:
        logger.warning(
            f"DataFrame is empty. Skipping upload to gs://{bucket_name}/{destination_blob_name}."
        )
        return

    storage_client = storage.Client()
    blob = storage_client.bucket(bucket_name).blob(destination_blob_name)

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    logger.info(f"Uploaded → gs://{bucket_name}/{destination_blob_name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Ingest XEMA weather data + dimension tables and upload to GCS."
    )
    parser.add_argument(
        '--date',   type=str, required=True,
        help="Observation date to fetch in YYYY-MM-DD format."
    )
    parser.add_argument(
        '--bucket', type=str, required=True,
        help="Destination GCS bucket name."
    )
    parser.add_argument(
        '--skip-dims', action='store_true', default=False,
        help="Skip dimension table ingestion (useful for back-fill runs)."
    )
    args = parser.parse_args()

    try:
        logger.info("=== Starting data ingestion job ===")

        # 1. Weather observations
        df_weather = fetch_weather_data(args.date)
        upload_to_gcs(
            df_weather, args.bucket,
            f"raw/weather_data/{args.date}/data.parquet"
        )

        if not args.skip_dims:
            # 2. Variables dimension  (full-refresh daily, WRITE_TRUNCATE in BQ)
            df_vars = fetch_dim_variables()
            upload_to_gcs(
                df_vars, args.bucket,
                f"raw/dim_variables/latest/data.parquet"
            )

            # 3. Stations dimension  (full-refresh daily, WRITE_TRUNCATE in BQ)
            df_stations = fetch_dim_stations()
            upload_to_gcs(
                df_stations, args.bucket,
                f"raw/dim_stations/latest/data.parquet"
            )

        logger.info("=== Ingestion job completed successfully ===")

    except Exception as e:
        logger.error(f"Ingestion job failed: {e}")
        raise


if __name__ == "__main__":
    main()