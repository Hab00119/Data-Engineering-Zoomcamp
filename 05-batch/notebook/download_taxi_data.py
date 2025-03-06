import os
import requests
import argparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
import gzip

class NYCTaxiDataDownloader:
    def __init__(self, output_dir, base_url):
        """
        Initialize the NYC Taxi Data Downloader
        
        Args:
            output_dir (str): Base directory to store downloaded data
            base_url (str): Base URL for NYC taxi data
        """
        self.base_url = base_url
        self.output_dir = output_dir
        
        # Create the base output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _download_file(self, url, local_path):
        """Download file from URL"""
        try:
            self.logger.info(f"Downloading from {url} to {local_path}")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Check content type and size
            content_type = response.headers.get('Content-Type', '')
            content_length = int(response.headers.get('Content-Length', 0))
            
            if content_length == 0 or 'text/html' in content_type:
                self.logger.error(f"Received invalid response. Content-Type: {content_type}, Content-Length: {content_length}")
                # Print first 100 bytes of response for debugging
                self.logger.error(f"Response preview: {response.content[:10]}")
                return False
                
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        except Exception as e:
            self.logger.error(f"Error downloading from {url}: {str(e)}")
            return False

    def process_csv(self, year, month, service_type):
        """Download CSV file"""
        file_name = f"{service_type}_tripdata_{year}-{month:02d}.csv"
        url = f"{self.base_url}/{file_name}"
        month_dir = os.path.join(self.output_dir, f"{service_type}/{year}/{month:02d}")
        local_path = os.path.join(month_dir, file_name)

        try:
            self.logger.info(f"Downloading CSV file: {file_name}")
            success = self._download_file(url, local_path)
            
            if success:
                self.logger.info(f"Successfully downloaded {file_name} to {local_path}")
            return success
        except Exception as e:
            self.logger.error(f"Error processing CSV {file_name}: {str(e)}")
            return False

    def process_parquet(self, year, month, service_type):
        """Download Parquet file"""
        file_name = f"{service_type}_tripdata_{year}-{month:02d}.parquet"
        url = f"{self.base_url}/{file_name}"
        month_dir = os.path.join(self.output_dir, f"{service_type}/{year}/{month:02d}")
        local_path = os.path.join(month_dir, file_name)

        try:
            self.logger.info(f"Downloading Parquet file: {file_name}")
            success = self._download_file(url, local_path)
            
            if success:
                self.logger.info(f"Successfully downloaded {file_name} to {local_path}")
            return success
        except Exception as e:
            self.logger.error(f"Error processing Parquet {file_name}: {str(e)}")
            return False

    def process_csv_gz(self, year, month, service_type):
        """Download CSV.GZ file"""
        gz_file_name = f"{service_type}_tripdata_{year}-{month:02d}.csv.gz"
        url = f"{self.base_url}/{gz_file_name}"
        month_dir = os.path.join(self.output_dir, f"{service_type}/{year}/{month:02d}")
        local_path = os.path.join(month_dir, gz_file_name)

        try:
            self.logger.info(f"Downloading CSV.GZ file: {gz_file_name}")
            success = self._download_file(url, local_path)
            
            if success:
                self.logger.info(f"Successfully downloaded {gz_file_name} to {local_path}")
                
                # Optionally uncompress the file to csv
                # Uncomment these lines if you want to automatically extract the gzipped files
                # self.logger.info(f"Extracting {gz_file_name}")
                # csv_path = local_path.replace('.csv.gz', '.csv')
                # with gzip.open(local_path, 'rb') as f_in:
                #     with open(csv_path, 'wb') as f_out:
                #         f_out.write(f_in.read())
                # self.logger.info(f"Extracted to {csv_path}")

                # Convert CSV.GZ to Parquet
                # self.logger.info(f"Converting {gz_file_name} to Parquet format")
                # df = pd.read_csv(local_gz_path, compression='gzip')
                # df.to_parquet(local_parquet_path, engine='pyarrow')
                # self.logger.info(f"Converted to Parquet: {local_parquet_path}")
                
                # Optionally remove the original gz file after conversion to parquet
                # Uncomment the line below if you want to delete the original gz file
                # os.remove(local_gz_path)
                # self.logger.info(f"Removed original gz file: {local_gz_path}")
                
            return success
        except Exception as e:
            self.logger.error(f"Error processing CSV.GZ {gz_file_name}: {str(e)}")
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
        description='Download NYC Taxi data to local folders organized by month'
    )
    
    parser.add_argument(
        '--output-dir',
        required=True,
        help='Base directory to store downloaded data'
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
        help='File format to download'
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
    
    downloader = NYCTaxiDataDownloader(args.output_dir, args.base_url)
    successful_months = downloader.process_months(
        year=args.year,
        start_month=args.start_month,
        end_month=args.end_month,
        service_type=args.service_type,
        file_format=args.file_format,
        max_workers=args.workers
    )
    
    print("\nDownload Summary:")
    print(f"Successfully downloaded months: {successful_months}")
    print(f"Total successful: {len(successful_months)}")
    print(f"Total failed: {args.end_month - args.start_month + 1 - len(successful_months)}")
    print(f"Files saved to: {args.output_dir}")

if __name__ == "__main__":
    main()

# Basic usage example:
# python download_nyc_taxi_data.py --output-dir ./taxi_data --file-format parquet

# Specify different base URL and service type
# python download_nyc_taxi_data.py --output-dir ./taxi_data --base-url "https://d37ci6vzurychx.cloudfront.net/trip-data" --service-type green --file-format parquet

# Full example with all parameters
# python download_nyc_taxi_data.py \
#     --output-dir ./taxi_data/fhv_2019 \
#     --base-url "https://d37ci6vzurychx.cloudfront.net/trip-data" \
#     --service-type fhv \
#     --year 2019 \
#     --start-month 1 \
#     --end-month 12 \
#     --file-format parquet \
#     --workers 4

# Alternative source example
# python download_nyc_taxi_data.py \
#     --output-dir ./taxi_data/fhv_2019 \
#     --base-url "https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv" \
#     --service-type fhv \
#     --year 2019 \
#     --start-month 1 \
#     --end-month 12 \
#     --file-format csv.gz \
#     --workers 4