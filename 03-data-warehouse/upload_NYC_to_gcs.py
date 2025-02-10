import os
import requests
import argparse
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime
import logging

class NYCTaxiDataLoader:
    def __init__(self, gcp_credentials_path, bucket_name, base_url="https://d37ci6vzurychx.cloudfront.net/trip-data"):
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
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _generate_file_url(self, year, month):
        """Generate the URL for a specific year and month"""
        file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
        return f"{self.base_url}/{file_name}", file_name

    def download_file(self, year, month):
        """
        Download a single month's taxi data
        
        Args:
            year (int): Year of the data
            month (int): Month of the data
            
        Returns:
            tuple: (local_path, gcs_blob_name) if successful, None if failed
        """
        url, file_name = self._generate_file_url(year, month)
        local_path = f"/tmp/{file_name}"
        
        try:
            # Download the file
            self.logger.info(f"Downloading {file_name}")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            return local_path, file_name
            
        except Exception as e:
            self.logger.error(f"Error downloading {file_name}: {str(e)}")
            return None

    def upload_to_gcs(self, local_path, blob_name):
        """
        Upload a file to Google Cloud Storage
        
        Args:
            local_path (str): Path to local file
            blob_name (str): Name for the blob in GCS
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            blob = self.bucket.blob(blob_name)
            self.logger.info(f"Uploading {blob_name} to GCS")
            blob.upload_from_filename(local_path)
            
            # Clean up local file
            os.remove(local_path)
            return True
            
        except Exception as e:
            self.logger.error(f"Error uploading {blob_name}: {str(e)}")
            return False

    def process_month(self, year, month):
        """Process a single month's data (download and upload)"""
        result = self.download_file(year, month)
        if result:
            local_path, blob_name = result
            return self.upload_to_gcs(local_path, blob_name)
        return False

    def process_months(self, year, start_month=1, end_month=7, max_workers=4):
        """
        Process multiple months in parallel
        
        Args:
            year (int): Year of the data
            start_month (int): Starting month (default: 1)
            end_month (int): Ending month (default: 7)
            max_workers (int): Maximum number of parallel workers (default: 4)
            
        Returns:
            list: List of successfully processed months
        """
        successful_months = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create a list of futures for each month
            future_to_month = {
                executor.submit(self.process_month, year, month): month
                for month in range(start_month, end_month + 1)
            }
            
            # Process results as they complete
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
        description='Download NYC Yellow Taxi data and upload to Google Cloud Storage'
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
    
    # Validate month range
    if args.start_month > args.end_month:
        parser.error("start-month must be less than or equal to end-month")
    
    return args

def main():
    # Parse command line arguments
    args = parse_args()
    
    # Initialize and run the loader
    loader = NYCTaxiDataLoader(args.credentials, args.bucket)
    successful_months = loader.process_months(
        year=args.year,
        start_month=args.start_month,
        end_month=args.end_month,
        max_workers=args.workers
    )
    
    # Print summary
    print("\nSummary:")
    print(f"Successfully processed months: {successful_months}")
    print(f"Total successful: {len(successful_months)}")
    print(f"Total failed: {args.end_month - args.start_month + 1 - len(successful_months)}")

if __name__ == "__main__":
    main()

# python upload_NYC_to_gcs.py --credentials /workspaces/Data-Engineering-Zoomcamp/03-data-warehouse/de-zoomcamp-449719-097c7a824cf5.json --bucket your-bucket-name
# python /workspaces/Data-Engineering-Zoomcamp/03-data-warehouse/upload_NYC_to_gcs.py --credentials /workspaces/Data-Engineering-Zoomcamp/03-data-warehouse/de-zoomcamp-449719-097c7a824cf5.json --bucket habeeb-babat-kestra --year 2024 --start-month 1 --end-month 7 --workers 1
# python upload_NYC_to_gcs.py --credentials /workspaces/Data-Engineering-Zoomcamp/03-data-warehouse/de-zoomcamp-449719-097c7a824cf5.json --bucket your-bucket-name --workers 8
# python upload_NYC_to_gcs.py --help