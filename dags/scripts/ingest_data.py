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



def fetch_weather_data(target_date_str: str) -> pd.DataFrame:
    """Fetches weather data from the XEMA API for a specific date."""
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
    next_date = target_date + timedelta(days=1)
    
    where_clause = f"data_lectura >= '{target_date}T00:00:00' AND data_lectura < '{next_date}T00:00:00' AND codi_estacio IN ('D5', 'X8', 'AN', 'X4', 'X2')"
    
    url = "https://analisi.transparenciacatalunya.cat/resource/nzvn-apee.csv"
    params = {
        "$where": where_clause,
        "$limit": 500000
    }
    
    logger.info(f"Fetching data from XEMA API for date: {target_date}")
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    df = pd.read_csv(io.StringIO(response.text))
    logger.info(f"Successfully fetched {len(df)} rows.")
    return df

def upload_to_gcs(df: pd.DataFrame, bucket_name: str, destination_blob_name: str):
    """Uploads a pandas DataFrame to a Google Cloud Storage bucket object as a Parquet file."""
    if df.empty:
        logger.warning(f"DataFrame is empty. No data to upload to gs://{bucket_name}/{destination_blob_name}.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    logger.info(f"Successfully uploaded data to gs://{bucket_name}/{destination_blob_name}")

def main():
    parser = argparse.ArgumentParser(description="Ingest XEMA weather data and upload to GCS.")
    parser.add_argument('--date', type=str, required=True, help="Date to fetch data for in YYYY-MM-DD format")
    parser.add_argument('--bucket', type=str, required=True, help="Name of the destination GCS Bucket")
    
    args = parser.parse_args()
    
    try:
        logger.info("Starting data ingestion job.")
        
        # 1. Fetch data
        df = fetch_weather_data(args.date)
        
        # 2. Upload to GCS
        destination_path = f"raw/weather_data/data_lectura={args.date}/data.parquet"
        upload_to_gcs(df, args.bucket, destination_path)
        
        logger.info("Ingestion job completed successfully.")
        
    except Exception as e:
        logger.error(f"Ingestion job failed: {e}")
        raise

if __name__ == "__main__":
    main()
