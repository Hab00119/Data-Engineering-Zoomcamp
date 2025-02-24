import os
import requests
import argparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime
import logging

class NYCTaxiDataLoader:
    def __init__(self, gcp_credentials_path, bucket_name, base_url):
        """
        Initialize the NYC Taxi Data Loader
        
        Args:
            gcp_credentials_path (str): Path to GCP service account JSON file
            bucket_name (str): Name of the GCS bucket
            base_url (str): Base URL for NYC taxi data
        """
        self.base_url = base_url
        self.bucket_name = bucket_name
        self.credentials = service_account.Credentials.from_service_account_file(
            gcp_credentials_path
        )
        self.storage_client = storage.Client(credentials=self.credentials)
        self.bucket = self.storage_client.bucket(bucket_name)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _download_file(self, url, local_path):
        """Download file from URL"""
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        except Exception as e:
            self.logger.error(f"Error downloading from {url}: {str(e)}")
            return False

    def standardize_dataframe(self, df):
        """Standardize column data types in the dataframe"""
        self.logger.info("Standardizing column data types")
        
        # Define the columns that need standardization and their target types
        type_mapping = {
            'SR_Flag': 'float64',  # Standardize to float64
            'DOlocationID': 'float64',  # Standardize to float64
            'PUlocationID': 'float64',  # Also standardize this column to be safe
            # Add any other columns that need standardization
        }
        
        # Apply type conversions for columns that exist in the dataframe
        for column, dtype in type_mapping.items():
            if column in df.columns:
                try:
                    df[column] = df[column].astype(dtype)
                    self.logger.info(f"Converted {column} to {dtype}")
                except Exception as e:
                    self.logger.warning(f"Could not convert {column} to {dtype}: {str(e)}")
        
        return df

    def process_csv(self, year, month, service_type):
        """Process and upload CSV file with standardized column types"""
        file_name = f"{service_type}_tripdata_{year}-{month:02d}.csv"
        url = f"{self.base_url}/{file_name}"
        local_path = f"/tmp/{file_name}"
        standardized_path = f"/tmp/standardized_{file_name.replace('.csv', '.parquet')}"

        try:
            self.logger.info(f"Downloading CSV file: {file_name}")
            if self._download_file(url, local_path):
                # Read the CSV file
                df = pd.read_csv(local_path)
                
                # Standardize the column types
                df = self.standardize_dataframe(df)
                
                # Save as parquet with standardized types
                parquet_file_name = file_name.replace('.csv', '.parquet')
                df.to_parquet(standardized_path, engine='pyarrow')
                
                # Remove the original downloaded file
                os.remove(local_path)
                
                # Upload the standardized parquet file
                success = self.upload_to_gcs(standardized_path, parquet_file_name)
                return success
            return False
        except Exception as e:
            self.logger.error(f"Error processing CSV {file_name}: {str(e)}")
            if os.path.exists(local_path):
                os.remove(local_path)
            if os.path.exists(standardized_path):
                os.remove(standardized_path)
            return False

    def process_parquet(self, year, month, service_type):
        """Process and upload Parquet file with standardized column types"""
        file_name = f"{service_type}_tripdata_{year}-{month:02d}.parquet"
        url = f"{self.base_url}/{file_name}"
        local_path = f"/tmp/{file_name}"
        standardized_path = f"/tmp/standardized_{file_name}"

        try:
            self.logger.info(f"Downloading Parquet file: {file_name}")
            if self._download_file(url, local_path):
                # Read the parquet file
                df = pd.read_parquet(local_path)
                
                # Standardize the column types
                df = self.standardize_dataframe(df)
                
                # Save the standardized dataframe
                df.to_parquet(standardized_path, engine='pyarrow')
                
                # Remove the original downloaded file
                os.remove(local_path)
                
                # Upload the standardized file
                success = self.upload_to_gcs(standardized_path, file_name)
                return success
            return False
        except Exception as e:
            self.logger.error(f"Error processing Parquet {file_name}: {str(e)}")
            if os.path.exists(local_path):
                os.remove(local_path)
            if os.path.exists(standardized_path):
                os.remove(standardized_path)
            return False

    def process_csv_gz(self, year, month, service_type):
        """Process CSV.GZ file and convert to Parquet with standardized column types"""
        gz_file_name = f"{service_type}_tripdata_{year}-{month:02d}.csv.gz"
        parquet_file_name = gz_file_name.replace('.csv.gz', '.parquet')
        url = f"{self.base_url}/{gz_file_name}"
        local_gz_path = f"/tmp/{gz_file_name}"
        local_parquet_path = f"/tmp/{parquet_file_name}"

        try:
            self.logger.info(f"Downloading CSV.GZ file: {gz_file_name}")
            if not self._download_file(url, local_gz_path):
                return False

            self.logger.info(f"Converting {gz_file_name} to parquet")
            df = pd.read_csv(local_gz_path, compression='gzip')
            
            # Standardize the column types
            df = self.standardize_dataframe(df)
            
            df.to_parquet(local_parquet_path, engine='pyarrow')
            
            # Clean up gz file
            os.remove(local_gz_path)
            
            # Upload parquet file
            success = self.upload_to_gcs(local_parquet_path, parquet_file_name)
            return success
            
        except Exception as e:
            self.logger.error(f"Error processing CSV.GZ {gz_file_name}: {str(e)}")
            if os.path.exists(local_gz_path):
                os.remove(local_gz_path)
            if os.path.exists(local_parquet_path):
                os.remove(local_parquet_path)
            return False

    def upload_to_gcs(self, local_path, blob_name):
        """Upload file to Google Cloud Storage"""
        try:
            blob = self.bucket.blob(blob_name)
            self.logger.info(f"Uploading {blob_name} to GCS")
            blob.upload_from_filename(local_path)
            os.remove(local_path)
            return True
        except Exception as e:
            self.logger.error(f"Error uploading {blob_name}: {str(e)}")
            if os.path.exists(local_path):
                os.remove(local_path)
            return False

    def process_month(self, year, month, service_type, file_format):
        """Process a single month's data based on format"""
        format_processors = {
            'csv': self.process_csv,
            'parquet': self.process_parquet,
            'csv.gz': self.process_csv_gz
        }
        
        processor = format_processors.get(file_format)
        if not processor:
            self.logger.error(f"Unsupported file format: {file_format}")
            return False
            
        return processor(year, month, service_type)

    def process_months(self, year, start_month, end_month, service_type, file_format, max_workers=4):
        """Process multiple months in parallel"""
        successful_months = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_month = {
                executor.submit(self.process_month, year, month, service_type, file_format): month
                for month in range(start_month, end_month + 1)
            }
            
            for future in future_to_month:
                month = future_to_month[future]
                try:
                    if future.result():
                        successful_months.append(month)
                        self.logger.info(f"Successfully processed month {month}")
                    else:
                        self.logger.warning(f"Failed to process month {month}")
                except Exception as e:
                    self.logger.error(f"Error processing month {month}: {str(e)}")
        
        return successful_months

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Download NYC Taxi data and upload to Google Cloud Storage with standardized column types'
    )
    
    parser.add_argument(
        '--credentials',
        required=True,
        help='Path to GCP service account credentials JSON file'
    )
    
    parser.add_argument(
        '--bucket',
        required=True,
        help='Name of the GCS bucket'
    )
    
    parser.add_argument(
        '--base-url',
        default="https://d37ci6vzurychx.cloudfront.net/trip-data",
        help='Base URL for NYC taxi data'
    )
    
    parser.add_argument(
        '--service-type',
        choices=['yellow', 'green', 'fhv'],
        default='yellow',
        help='Type of taxi service data to download'
    )
    
    parser.add_argument(
        '--file-format',
        choices=['csv', 'parquet', 'csv.gz'],
        required=True,
        help='File format to download and process'
    )
    
    parser.add_argument(
        '--year',
        type=int,
        default=datetime.now().year,
        help='Year of data to download (default: current year)'
    )
    
    parser.add_argument(
        '--start-month',
        type=int,
        default=1,
        choices=range(1, 13),
        help='Starting month (1-12, default: 1)'
    )
    
    parser.add_argument(
        '--end-month',
        type=int,
        default=7,
        choices=range(1, 13),
        help='Ending month (1-12, default: 7)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )
    
    args = parser.parse_args()
    
    if args.start_month > args.end_month:
        parser.error("start-month must be less than or equal to end-month")
    
    return args

def main():
    args = parse_args()
    
    loader = NYCTaxiDataLoader(args.credentials, args.bucket, args.base_url)
    successful_months = loader.process_months(
        year=args.year,
        start_month=args.start_month,
        end_month=args.end_month,
        service_type=args.service_type,
        file_format=args.file_format,
        max_workers=args.workers
    )
    
    print("\nSummary:")
    print(f"Successfully processed months: {successful_months}")
    print(f"Total successful: {len(successful_months)}")
    print(f"Total failed: {args.end_month - args.start_month + 1 - len(successful_months)}")

if __name__ == "__main__":
    main()


# Basic usage with default base URL
# python upload_NYC_to_gcs.py --credentials path/to/credentials.json --bucket your-bucket-name --file-format parquet

# Specify different base URL and service type
# python upload_NYC_to_gcs.py --credentials path/to/credentials.json --bucket your-bucket-name --base-url "https://your-custom-url.com/data" --service-type green --file-format parquet

# Full example with all parameters
# python /workspaces/Data-Engineering-Zoomcamp/03-data-warehouse/upload_NYC_to_gcs3.py \
#     --credentials /workspaces/Data-Engineering-Zoomcamp/03-data-warehouse/new-de-zoomcamp-449719-9c27773d9a31.json \
#     --bucket habeeb-babat-kestra \
#     --base-url "https://d37ci6vzurychx.cloudfront.net/trip-data" \
#     --service-type fhv \
#     --year 2019 \
#     --start-month 1 \
#     --file-format parquet \
#     --end-month 12 \
#     --workers 1