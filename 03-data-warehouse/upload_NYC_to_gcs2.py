import os
import requests
import argparse
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
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _generate_file_urls(self, year, month, service_type="yellow"):
        """
        Generate URLs for all supported file formats
        
        Args:
            year (int): Year of the data
            month (int): Month of the data
            service_type (str): Type of taxi service (yellow, green, fhv)
            
        Returns:
            list: List of tuples containing (url, filename) for each format
        """
        formats = [
            ("parquet", "parquet"),
            ("csv", "csv"),
            ("csv.gz", "csv.gz")
        ]
        
        urls = []
        for ext, file_ext in formats:
            file_name = f"{service_type}_tripdata_{year}-{month:02d}.{file_ext}"
            url = f"{self.base_url}/{file_name}"
            urls.append((url, file_name))
        
        return urls

    def download_file(self, year, month, service_type="yellow"):
        """
        Try to download data in available formats
        
        Args:
            year (int): Year of the data
            month (int): Month of the data
            service_type (str): Type of taxi service
            
        Returns:
            tuple: (local_path, gcs_blob_name) if successful, None if failed
        """
        urls = self._generate_file_urls(year, month, service_type)
        
        for url, file_name in urls:
            local_path = f"/tmp/{file_name}"
            
            try:
                # Try to download the file
                self.logger.info(f"Attempting to download {file_name}")
                response = requests.get(url, stream=True)
                response.raise_for_status()
                
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                self.logger.info(f"Successfully downloaded {file_name}")
                return local_path, file_name
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Failed to download {file_name}: {str(e)}")
                continue
            except Exception as e:
                self.logger.error(f"Error processing {file_name}: {str(e)}")
                continue
        
        self.logger.error(f"Failed to download data for {year}-{month:02d} in any format")
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

    def process_month(self, year, month, service_type="yellow"):
        """Process a single month's data (download and upload)"""
        result = self.download_file(year, month, service_type)
        if result:
            local_path, blob_name = result
            return self.upload_to_gcs(local_path, blob_name)
        return False

    def process_months(self, year, start_month=1, end_month=7, max_workers=4, service_type="yellow"):
        """
        Process multiple months in parallel
        
        Args:
            year (int): Year of the data
            start_month (int): Starting month (default: 1)
            end_month (int): Ending month (default: 7)
            max_workers (int): Maximum number of parallel workers (default: 4)
            service_type (str): Type of taxi service (default: "yellow")
            
        Returns:
            list: List of successfully processed months
        """
        successful_months = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_month = {
                executor.submit(self.process_month, year, month, service_type): month
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
        description='Download NYC Taxi data and upload to Google Cloud Storage'
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
        max_workers=args.workers,
        service_type=args.service_type
    )
    
    print("\nSummary:")
    print(f"Successfully processed months: {successful_months}")
    print(f"Total successful: {len(successful_months)}")
    print(f"Total failed: {args.end_month - args.start_month + 1 - len(successful_months)}")

if __name__ == "__main__":
    main()


# Basic usage with default base URL
#python upload_NYC_to_gcs.py --credentials path/to/credentials.json --bucket your-bucket-name

# Specify different base URL and service type
#python upload_NYC_to_gcs.py --credentials path/to/credentials.json --bucket your-bucket-name --base-url "https://your-custom-url.com/data" --service-type green

# Full example with all parameters
# python /workspaces/Data-Engineering-Zoomcamp/03-data-warehouse/upload_NYC_to_gcs2.py \
#     --credentials /workspaces/Data-Engineering-Zoomcamp/03-data-warehouse/new-de-zoomcamp-449719-9c27773d9a31.json \
#     --bucket habeeb-kestra \
#     --base-url "https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv" \
#     --service-type fhv \
#     --year 2019 \
#     --start-month 1 \
#     --end-month 12 \
#     --workers 1