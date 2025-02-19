import os
import requests
import argparse
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime
import logging
import dlt
from dlt.sources.credentials import GcpServiceAccountCredentials
from dlt.destinations import bigquery
import json

###############################
# NYCTaxiDataLoader Definition
###############################
class NYCTaxiDataLoader:
    def __init__(self, gcp_credentials_path, bucket_name, file_format, base_url="https://d37ci6vzurychx.cloudfront.net/trip-data"):
        """
        Initialize the NYC Taxi Data Loader.
        
        Args:
            gcp_credentials_path (str): Path to GCP service account JSON file.
            bucket_name (str): Name of the GCS bucket.
            file_format (str): File format to download ('parquet', 'csv', or 'excel').
            base_url (str): Base URL for NYC taxi data.
        """
        self.base_url = base_url
        self.bucket_name = bucket_name
        self.file_format = file_format.lower()
        self.credentials = service_account.Credentials.from_service_account_file(gcp_credentials_path)
        self.storage_client = storage.Client(credentials=self.credentials)
        self.bucket = self.storage_client.bucket(bucket_name)
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _generate_file_url(self, year, month):
        """
        Generate the file URL for a given month/year.
        For Excel files, the extension is assumed to be 'xlsx'.
        """
        ext = self.file_format
        if ext == "excel":
            ext = "xlsx"
        file_name = f"yellow_tripdata_{year}-{month:02d}.{ext}"
        return f"{self.base_url}/{file_name}", file_name

    def download_file(self, year, month):
        """
        Download a single month's taxi data.
        """
        url, file_name = self._generate_file_url(year, month)
        local_path = f"/tmp/{file_name}"
        try:
            self.logger.info(f"Downloading {file_name} from {url}")
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
        Upload the local file to GCS.
        """
        try:
            blob = self.bucket.blob(blob_name)
            self.logger.info(f"Uploading {blob_name} to GCS bucket {self.bucket_name}")
            blob.upload_from_filename(local_path)
            os.remove(local_path)
            return True
        except Exception as e:
            self.logger.error(f"Error uploading {blob_name}: {str(e)}")
            return False

    def process_month(self, year, month):
        """
        Process a single month (download then upload).
        """
        result = self.download_file(year, month)
        if result:
            local_path, blob_name = result
            return self.upload_to_gcs(local_path, blob_name)
        return False

    def process_months(self, year, start_month=1, end_month=7, max_workers=4):
        """
        Process multiple months in parallel.
        """
        successful_months = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_month = {
                executor.submit(self.process_month, year, month): month
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

#################################
# Command-Line Argument Parsing
#################################
def parse_args():
    parser = argparse.ArgumentParser(
        description="NYCTaxiDataLoader + DLT Pipeline: Download NYC taxi data (in a single specified format) to GCS or run a DLT pipeline to load it to BigQuery."
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
        help='Starting month (1-12)'
    )
    parser.add_argument(
        '--end-month',
        type=int,
        default=7,
        choices=range(1, 13),
        help='Ending month (1-12)'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )
    parser.add_argument(
        '--file-format',
        type=str,
        required=True,
        choices=['parquet', 'csv', 'excel'],
        help='File format to process: parquet, csv, or excel'
    )
    parser.add_argument(
        '--action',
        type=str,
        required=True,
        choices=['download', 'pipeline'],
        help='Action to perform: "download" to load files into GCS, or "pipeline" to run the DLT pipeline to load from GCS to BigQuery'
    )
    return parser.parse_args()

#################################
# DLT Pipeline Code
#################################
# Import DLT libraries and filesystem utilities
import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem, read_parquet, read_csv
from typing import Iterator

def get_bucket_url(bucket_name: str) -> str:
    """
    Construct the filesystem URL for GCS.
    (For GCS, the URL scheme is typically "gs://")
    """
    return f"gs://{bucket_name}"

# Custom transformer to read Excel files using yield.
@dlt.transformer(standalone=True)
def read_excel(items: Iterator[FileItemDict], sheet_name: str) -> Iterator[TDataItems]:
    import pandas as pd
    for file_obj in items:
        with file_obj.open() as f:
            df = pd.read_excel(f, sheet_name=sheet_name)
            for record in df.to_dict(orient="records"):
                yield record

def run_dlt_pipeline(bucket_name: str, file_format: str,gcp_credentials_path: str,  sheet_name: str = "Sheet1"):
    """
    Run the DLT pipeline that reads files from the GCS bucket and loads them to BigQuery.
    Only files matching the specified file format are processed.
    """
    BUCKET_URL = get_bucket_url(bucket_name)

    #  we use gcp_credentials for both source and destination credentials https://dlthub.com/docs/general-usage/credentials/advanced
    with open(gcp_credentials_path, "r") as f:
        creds2 = json.load(f)
    
    gcp_credentials = GcpServiceAccountCredentials()
    gcp_credentials.parse_native_representation(json.dumps(creds2))

    # Choose the appropriate file resource based on the file_format.
    if file_format.lower() == "csv":
        resource = filesystem(
            bucket_url=BUCKET_URL,
            file_glob="*.csv",
            credentials=gcp_credentials
        ) | read_csv()
    elif file_format.lower() == "parquet":
        resource = filesystem(
            bucket_url=BUCKET_URL,
            file_glob="*.parquet",
            credentials=gcp_credentials
        ) | read_parquet()
    elif file_format.lower() == "excel":
        resource = filesystem(
            bucket_url=BUCKET_URL,
            file_glob="*.xlsx",
            credentials=gcp_credentials
        ) | read_excel(sheet_name)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    
    # Define the DLT pipeline. In this example, we load into BigQuery.
    pipeline = dlt.pipeline(
        pipeline_name="nyc_taxi_pipeline",
        destination=bigquery(credentials=gcp_credentials),
        dataset_name="nyc_taxi_data"
    )
    
    # Run the pipeline loading the data into a table called "nyc_taxi_table".
    load_info = pipeline.run(resource.with_name("nyc_taxi_table"))
    #load_info = pipeline.run(resource, table_name="nyc_taxi_table", write_disposition="replace")
    print(load_info)

###############################
# Main Function
###############################
def main():
    args = parse_args()

    # The user-specified file format controls both the download/upload and the DLT processing.
    if args.action == "download":
        loader = NYCTaxiDataLoader(args.credentials, args.bucket, args.file_format)
        successful_months = loader.process_months(
            year=args.year,
            start_month=args.start_month,
            end_month=args.end_month,
            max_workers=args.workers
        )
        total_months = args.end_month - args.start_month + 1
        print("\nSummary:")
        print(f"Successfully processed months: {successful_months}")
        print(f"Total successful: {len(successful_months)}")
        print(f"Total failed: {total_months - len(successful_months)}")
    elif args.action == "pipeline":
        # For Excel files, you can adjust the sheet name as needed.
        run_dlt_pipeline(args.bucket, args.file_format, args.credentials, sheet_name="Sheet1")
    else:
        print("Invalid action specified. Use 'download' or 'pipeline'.")

if __name__ == "__main__":
    main()