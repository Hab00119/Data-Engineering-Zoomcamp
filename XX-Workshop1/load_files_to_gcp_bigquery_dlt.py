import os
import requests
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime
import json
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import re

# DLT imports
import dlt
from dlt.sources.credentials import GcpServiceAccountCredentials
from dlt.destinations import bigquery
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem
from typing import Iterator, List, Optional, Union, Literal

###############################
# Setup Logging
###############################
def setup_logging(level=logging.INFO):
    """Set up logging configuration"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Return the root logger
    return logging.getLogger()

logger = setup_logging()

###############################
# NYCTaxiDataLoader Definition
###############################
class NYCTaxiDataLoader:
    def __init__(self, gcp_credentials_path, bucket_name, file_format, data_type="yellow", 
                 base_url="https://d37ci6vzurychx.cloudfront.net/trip-data"):
        """
        Initialize the NYC Taxi Data Loader.
        
        Args:
            gcp_credentials_path (str): Path to GCP service account JSON file.
            bucket_name (str): Name of the GCS bucket.
            file_format (str): File format to download ('parquet', 'csv', or 'excel').
            data_type (str): Type of taxi data to load ('yellow', 'green', or 'both').
            base_url (str): Base URL for NYC taxi data.
        """
        self.base_url = base_url
        self.bucket_name = bucket_name
        self.file_format = file_format.lower()
        self.data_type = data_type.lower()
        self.credentials = service_account.Credentials.from_service_account_file(gcp_credentials_path)
        self.storage_client = storage.Client(credentials=self.credentials)
        self.bucket = self.storage_client.bucket(bucket_name)
        self.logger = logging.getLogger("NYCTaxiDataLoader")
        
        self.logger.info(f"Initialized loader for {self.data_type} taxi data in {self.file_format} format")

    def _get_data_types_to_process(self):
        """
        Returns a list of data types to process based on the data_type setting.
        """
        if self.data_type == "both":
            return ["yellow", "green"]
        return [self.data_type]

    def _generate_file_url(self, data_type, year, month):
        """
        Generate the file URL for a given data type, year, and month.
        For Excel files, the extension is assumed to be 'xlsx'.
        """
        ext = self.file_format
        if ext == "excel":
            ext = "xlsx"
        file_name = f"{data_type}_tripdata_{year}-{month:02d}.{ext}"
        return f"{self.base_url}/{file_name}", file_name

    def download_file(self, data_type, year, month):
        """
        Download a single month's taxi data.
        """
        url, file_name = self._generate_file_url(data_type, year, month)
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

    def process_file(self, data_type, year, month):
        """
        Process a single file (download then upload).
        """
        result = self.download_file(data_type, year, month)
        if result:
            local_path, blob_name = result
            return self.upload_to_gcs(local_path, blob_name)
        return False

    def process_months(self, year, start_month=1, end_month=7, max_workers=4):
        """
        Process multiple months in parallel for all selected data types.
        """
        data_types = self._get_data_types_to_process()
        self.logger.info(f"Processing data types: {data_types}")
        
        successful_files = []
        total_files = len(data_types) * (end_month - start_month + 1)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            
            # Submit all tasks
            for data_type in data_types:
                for month in range(start_month, end_month + 1):
                    future = executor.submit(self.process_file, data_type, year, month)
                    futures[future] = (data_type, month)
            
            # Process results
            for future in futures:
                data_type, month = futures[future]
                file_id = f"{data_type}-{year}-{month:02d}"
                try:
                    if future.result():
                        successful_files.append(file_id)
                        self.logger.info(f"Successfully processed {file_id}")
                    else:
                        self.logger.warning(f"Failed to process {file_id}")
                except Exception as e:
                    self.logger.error(f"Error processing {file_id}: {str(e)}")
        
        return successful_files, total_files

#################################
# DLT Pipeline Optimized Functions
#################################
@dlt.transformer(standalone=True)
def read_parquet_optimized(items: Iterator[FileItemDict], data_type: Optional[str] = None,
                          year: Optional[int] = None, months: Optional[List[int]] = None,
                          batch_size: int = 100000) -> Iterator[TDataItems]:
    """
    Read Parquet files in an optimized way using `pyarrow.parquet.ParquetFile`.
    Processes files in batches to save memory.
    """

    dlt_logger = logging.getLogger("dlt.parquet_reader")
    pattern = r'(yellow|green)_tripdata_(\d{4})-(\d{2})\.parquet'
    
    for file_obj in items:
        logger.info(f"FileItemDict content: {file_obj}")
        file_name = file_obj["file_name"]
        dlt_logger.info(f"Evaluating file: {file_name}")

        match = re.match(pattern, file_name)
        if match:
            file_data_type, file_year, file_month = match.group(1), int(match.group(2)), int(match.group(3))

            # Skip files that don't match filter criteria
            if ((data_type and data_type != "both" and file_data_type != data_type) or
                (year and file_year != year) or 
                (months and int(file_month) not in months)):
                dlt_logger.info(f"Skipping {file_name} - not matching filter criteria")
                continue

            dlt_logger.info(f"Processing {file_name}")
            try:
                with file_obj.open() as f:
                    table = pq.ParquetFile(f)
                    
                    for batch in table.iter_batches(batch_size=batch_size):
                        df = pa.Table.from_batches([batch]).to_pandas()
                        
                        # Convert datetime columns to string to avoid JSON serialization errors
                        # for col in df.select_dtypes(include=['datetime64']).columns:
                        #     df[col] = df[col].astype(str)

                        df['taxi_type'] = file_data_type  # Add source column

                        dlt_logger.info(f"Processing {len(df)} records from {file_name}")
                        yield df.to_dict(orient="records")

                        # for record in df.to_dict(orient="records"):
                        #     yield record

                dlt_logger.info(f"Completed processing {file_name}")
            except Exception as e:
                dlt_logger.error(f"Error processing {file_name}: {str(e)}")
        else:
            dlt_logger.warning(f"File {file_name} doesn't match expected pattern - skipping")

@dlt.transformer(standalone=True)
def read_csv_optimized(items: Iterator[FileItemDict], data_type: Optional[str] = None,
                      year: Optional[int] = None, months: Optional[List[int]] = None) -> Iterator[TDataItems]:
    """
    Read CSV files with selective processing based on data type, year and months.
    Uses yield to conserve memory by processing one file at a time.
    """
    dlt_logger = logging.getLogger("dlt.csv_reader")
    pattern = r'(yellow|green)_tripdata_(\d{4})-(\d{2})\.csv'
    
    for file_obj in items:
        #file_name = file_obj.name.split('/')[-1]
        logger.info(f"FileItemDict content: {file_obj}")
        file_name = file_obj["file_name"]
        dlt_logger.info(f"Evaluating file: {file_name}")
        
        # Filter by data type, year and month if specified
        match = re.match(pattern, file_name)
        if match:
            file_data_type, file_year, file_month = match.group(1), int(match.group(2)), int(match.group(3))
            
            # Skip files that don't match our filter criteria
            if ((data_type and data_type != "both" and file_data_type != data_type) or
                (year and file_year != year) or 
                (months and int(file_month) not in months)):
                dlt_logger.info(f"Skipping {file_name} - not matching filter criteria")
                continue
            
            dlt_logger.info(f"Processing {file_name}")
            try:
                with file_obj.open() as f:
                    # Use pandas to read csv in chunks to save memory
                    for chunk_num, chunk in enumerate(pd.read_csv(f, chunksize=100000)):
                        # Add a source column to identify the data type
                        chunk['taxi_type'] = file_data_type
                        dlt_logger.debug(f"Processing chunk {chunk_num+1} with {len(chunk)} records from {file_name}")
                        yield chunk.to_dict(orient="records")
                        # for record in chunk.to_dict(orient="records"):
                        #     yield record
                dlt_logger.info(f"Completed processing {file_name}")
            except Exception as e:
                dlt_logger.error(f"Error processing {file_name}: {str(e)}")
        else:
            dlt_logger.warning(f"File {file_name} doesn't match expected pattern - skipping")

@dlt.transformer(standalone=True)
def read_excel_optimized(items: Iterator[FileItemDict], sheet_name: str = "Sheet1", 
                        data_type: Optional[str] = None, year: Optional[int] = None, 
                        months: Optional[List[int]] = None) -> Iterator[TDataItems]:
    """
    Read Excel files with selective processing based on data type, year and months.
    Uses yield to conserve memory by processing one file at a time.
    """
    dlt_logger = logging.getLogger("dlt.excel_reader")
    pattern = r'(yellow|green)_tripdata_(\d{4})-(\d{2})\.xlsx'
    
    for file_obj in items:
        #file_name = file_obj.name.split('/')[-1]
        logger.info(f"FileItemDict content: {file_obj}")
        file_name = file_obj["file_name"]
        dlt_logger.info(f"Evaluating file: {file_name}")
        
        # Filter by data type, year and month if specified
        match = re.match(pattern, file_name)
        if match:
            file_data_type, file_year, file_month = match.group(1), int(match.group(2)), int(match.group(3))
            
            # Skip files that don't match our filter criteria
            if ((data_type and data_type != "both" and file_data_type != data_type) or
                (year and file_year != year) or 
                (months and int(file_month) not in months)):
                dlt_logger.info(f"Skipping {file_name} - not matching filter criteria")
                continue
            
            dlt_logger.info(f"Processing {file_name}")
            try:
                with file_obj.open() as f:
                    df = pd.read_excel(f, sheet_name=sheet_name)
                    # Add a source column to identify the data type
                    df['taxi_type'] = file_data_type
                    dlt_logger.info(f"Loaded {len(df)} records from {file_name}")
                    
                    # Process in chunks to save memory
                    chunk_size = 100000
                    total_rows = len(df)
                    for i in range(0, total_rows, chunk_size):
                        chunk = df.iloc[i:min(i+chunk_size, total_rows)]
                        dlt_logger.debug(f"Processing chunk {i//chunk_size + 1} with {len(chunk)} records from {file_name}")
                        yield chunk.to_dict(orient="records")
                        # for record in chunk.to_dict(orient="records"):
                        #     yield record
                dlt_logger.info(f"Completed processing {file_name}")
            except Exception as e:
                dlt_logger.error(f"Error processing {file_name}: {str(e)}")
        else:
            dlt_logger.warning(f"File {file_name} doesn't match expected pattern - skipping")

def get_bucket_url(bucket_name: str) -> str:
    """
    Construct the filesystem URL for GCS.
    """
    return f"gs://{bucket_name}"

def run_dlt_pipeline(bucket_name: str, file_format: str, gcp_credentials_path: str, 
                    data_type: str = "yellow", year: Optional[int] = None, 
                    months: Optional[List[int]] = None, sheet_name: str = "Sheet1"):
    """
    Run the DLT pipeline that reads files from the GCS bucket and loads them to BigQuery.
    Optimized to selectively process files based on data type, year and months.
    
    Args:
        bucket_name: Name of the GCS bucket
        file_format: One of 'parquet', 'csv', or 'excel'
        gcp_credentials_path: Path to GCP service account JSON
        data_type: Type of taxi data ('yellow', 'green', or 'both')
        year: Optional year to filter files
        months: Optional list of months to filter files
        sheet_name: For Excel files, the sheet name to read
    """
    pipeline_logger = logging.getLogger("dlt.pipeline")
    pipeline_logger.info(f"Starting DLT pipeline for {data_type} taxi data in {file_format} format from {bucket_name}")
    
    if months:
        pipeline_logger.info(f"Filtering for data_type={data_type}, year={year}, months={months}")
    
    BUCKET_URL = get_bucket_url(bucket_name)
    
    # Load GCP credentials
    with open(gcp_credentials_path, "r") as f:
        creds_dict = json.load(f)
    
    gcp_credentials = GcpServiceAccountCredentials()
    gcp_credentials.parse_native_representation(json.dumps(creds_dict))
    
    # Choose the appropriate file resource based on the file_format
    file_extension = ".xlsx" if file_format.lower() == "excel" else f".{file_format.lower()}"
    
    # Set file glob based on data type
    if data_type == "both":
        file_glob = f"*_tripdata_*{file_extension}"
    else:
        file_glob = f"{data_type}_tripdata_*{file_extension}"
    
    pipeline_logger.info(f"Setting up filesystem resource with glob pattern: {file_glob}")
    fs_resource = filesystem(
        bucket_url=BUCKET_URL,
        file_glob=file_glob,
        credentials=gcp_credentials
    )
    
    # Choose the appropriate transformer based on file format
    if file_format.lower() == "csv":
        pipeline_logger.info("Using optimized CSV reader")
        resource = fs_resource | read_csv_optimized(data_type=data_type, year=year, months=months)
    elif file_format.lower() == "parquet":
        pipeline_logger.info("Using optimized Parquet reader")
        resource = fs_resource | read_parquet_optimized(data_type=data_type, year=year, months=months)
    elif file_format.lower() == "excel":
        pipeline_logger.info(f"Using optimized Excel reader with sheet: {sheet_name}")
        resource = fs_resource | read_excel_optimized(sheet_name=sheet_name, data_type=data_type, 
                                                     year=year, months=months)
    else:
        error_msg = f"Unsupported file format: {file_format}"
        pipeline_logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Define the table name based on data type
    table_name = f"nyc_{data_type}_taxi_table" if data_type != "both" else "nyc_taxi_table"
    
    # Define and run the DLT pipeline
    pipeline_name = f"nyc_taxi_{data_type}_{file_format}_pipeline"
    pipeline_logger.info(f"Initializing pipeline: {pipeline_name}")
    
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=bigquery(credentials=gcp_credentials),
        dataset_name="nyc_taxi_data"
    )
    
    pipeline_logger.info(f"Starting pipeline run for table {table_name}")
    try:
        load_info = pipeline.run(resource.with_name(table_name))
        pipeline_logger.info(f"Pipeline completed successfully: {load_info}")
        #pipeline_logger.info(f"Loaded {load_info.load_package.normalized_rows_count} rows")
        return load_info
    except Exception as e:
        pipeline_logger.error(f"Pipeline failed: {str(e)}")
        raise

#################################
# Command-Line Argument Parsing
#################################
def parse_args():
    parser = argparse.ArgumentParser(
        description="NYCTaxiDataLoader + DLT Pipeline: Download NYC taxi data to GCS or run an optimized DLT pipeline."
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
        help='Year of data to download/process (default: current year)'
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
        default=6,
        choices=range(1, 13),
        help='Ending month (1-12)'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers for download (default: 4)'
    )
    parser.add_argument(
        '--file-format',
        type=str,
        required=True,
        choices=['parquet', 'csv', 'excel'],
        help='File format to process: parquet, csv, or excel'
    )
    parser.add_argument(
        '--data-type',
        type=str,
        default='yellow',
        choices=['yellow', 'green', 'both'],
        help='Type of taxi data to process (default: yellow)'
    )
    parser.add_argument(
        '--action',
        type=str,
        required=True,
        choices=['download', 'pipeline'],
        help='Action to perform: "download" to load files into GCS, or "pipeline" to run the DLT pipeline'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set the logging level'
    )
    return parser.parse_args()

###############################
# Main Function
###############################
def main():
    args = parse_args()
    
    # Set up logging with requested level
    log_level = getattr(logging, args.log_level)
    setup_logging(level=log_level)
    
    logger.info(f"Starting application with action={args.action}, data_type={args.data_type}, format={args.file_format}")
    
    # Get the months list for pipeline filtering
    months_list = list(range(args.start_month, args.end_month + 1))
    
    if args.action == "download":
        logger.info(f"Downloading {args.data_type} NYC taxi data for {args.year}, months {args.start_month}-{args.end_month}")
        loader = NYCTaxiDataLoader(args.credentials, args.bucket, args.file_format, data_type=args.data_type)
        successful_files, total_files = loader.process_months(
            year=args.year,
            start_month=args.start_month,
            end_month=args.end_month,
            max_workers=args.workers
        )
        logger.info("\nSummary:")
        logger.info(f"Successfully processed files: {successful_files}")
        logger.info(f"Total successful: {len(successful_files)}")
        logger.info(f"Total failed: {total_files - len(successful_files)}")
    elif args.action == "pipeline":
        logger.info(f"Running DLT pipeline for {args.data_type} taxi data in {args.file_format} format")
        logger.info(f"Filtering for year={args.year}, months={months_list}")
        # Run optimized pipeline with data type, year, and month filtering
        run_dlt_pipeline(
            args.bucket, 
            args.file_format, 
            args.credentials,
            data_type=args.data_type,
            year=args.year,
            months=months_list,
            sheet_name="Sheet1"
        )
    else:
        logger.error("Invalid action specified. Use 'download' or 'pipeline'.")

if __name__ == "__main__":
    main()