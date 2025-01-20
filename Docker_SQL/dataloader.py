import os
import sys
from time import time
from typing import Optional, Generator, Union, Any
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
import logging
from pathlib import Path
import argparse
from urllib.parse import urlparse
import wget
import psutil
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.exc import SQLAlchemyError
import numpy as np

class NYCTripDataLoader:
    """
    Optimized class to download and load NYC trip data into a PostgreSQL database.
    """
    def __init__(self, 
                 url: str, 
                 user: str, 
                 password: str, 
                 host: str, 
                 port: str, 
                 db: str, 
                 table_name: str, 
                 batch_size: Optional[int] = None,
                 max_workers: int = 4):
        """
        Initialize the data loader with optimized parameters.
        
        Args:
            url: URL of the NYC trip data file
            user: PostgreSQL username
            password: PostgreSQL password
            host: Database host
            port: Database port
            db: Database name
            table_name: Destination table name
            batch_size: Number of rows per batch (auto-calculated if None)
            max_workers: Maximum number of parallel workers
        """
        self.url = url
        self.table_name = table_name
        self.max_workers = max_workers
        self.filename = urlparse(url).path.split('/')[-1]
        
        # Set up enhanced logging
        self._setup_logging()
        
        # Initialize database connection
        self.engine = self._create_db_connection(user, password, host, port, db)
        
        # Auto-calculate optimal batch size if not provided
        self.batch_size = batch_size or self._calculate_optimal_batch_size()
        
        # Track performance metrics
        self.metrics = {
            'download_time': 0,
            'processing_time': 0,
            'total_rows': 0,
            'batches_processed': 0
        }

    def _setup_logging(self) -> None:
        """Configure detailed logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'nyc_data_loader_{time()}.log')
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _create_db_connection(self, user: str, password: str, host: str, 
                            port: str, db: str) -> create_engine:
        """Create database connection with optimized settings."""
        return create_engine(
            f'postgresql://{user}:{password}@{host}:{port}/{db}',
            pool_size=self.max_workers,
            max_overflow=2,
            pool_timeout=30,
            pool_recycle=1800
        )

    def _calculate_optimal_batch_size(self) -> int:
        """Calculate optimal batch size based on available system memory."""
        available_memory = psutil.virtual_memory().available
        estimated_row_size = 1000  # Estimated bytes per row
        optimal_size = (available_memory * 0.2) // estimated_row_size  # Use 20% of available memory
        return min(int(optimal_size), 100000)  # Cap at 100k rows

    def download_data(self) -> None:
        """Download data with progress tracking and error handling."""
        if os.path.exists(self.filename):
            self.logger.info(f"File {self.filename} already exists, skipping download")
            return

        try:
            start_time = time()
            self.logger.info(f"Downloading {self.filename}")
            
            wget.download(self.url, self.filename)
            
            self.metrics['download_time'] = time() - start_time
            self.logger.info(f"\nDownload completed in {self.metrics['download_time']:.2f} seconds")
            
        except Exception as e:
            self.logger.error(f"Download failed: {str(e)}")
            raise

    def _get_data_iterator(self) -> Generator:
        """Create optimized data iterator based on file type."""
        if '.parquet' in self.filename:
            table = pq.ParquetFile(self.filename)
            return table.iter_batches(self.batch_size)
        elif '.csv' in self.filename:
            return pd.read_csv(
                self.filename,
                chunksize=self.batch_size,
                dtype_backend='pyarrow'  # Use Arrow for better performance
            )
        else:
            raise ValueError("Unsupported file format. Use .csv or .parquet files.")

    def _prepare_table_schema(self) -> None:
        """Prepare database table with optimized schema."""
        try:
            # Read sample data
            #self.logger.info(f"Got here")
            if '.parquet' in self.filename:
                df_sample = next(pq.ParquetFile(self.filename).iter_batches(batch_size=1)).to_pandas()
            else:
                df_sample = pd.read_csv(self.filename, nrows=1)

            #self.logger.info(f"Got here 2")
            # Create table with optimized datatypes
            with self.engine.begin() as conn:
                df_sample.head(0).to_sql(
                    name=self.table_name,
                    con=conn,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
            #self.logger.info(f"Got here 3")
                # Create indexes if needed (customize based on your needs)
                #conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_pickup_time ON {self.table_name} (pickup_datetime)"))

        except Exception as e:
            self.logger.error(f"Failed to prepare table schema: {str(e)}")
            raise

    def _process_batch(self, batch: Union[pd.DataFrame, Any]) -> None:
        """Process a single batch with error handling."""
        try:
            if hasattr(batch, 'to_pandas'):  # RecordBatch has to_pandas method
                df = batch.to_pandas()
            else:
                df = batch

            with self.engine.begin() as conn:
                df.to_sql(
                    name=self.table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
            
            self.metrics['total_rows'] += len(df)
            self.metrics['batches_processed'] += 1

        except Exception as e:
            self.logger.error(f"Batch processing failed: {str(e)}")
            raise

    def load_and_insert_data(self) -> None:
        """Load and insert data with parallel processing."""
        start_time = time()
        
        try:
            self.logger.info("Preparing table schema...")
            self._prepare_table_schema()
            
            self.logger.info(f"Processing data in batches of {self.batch_size:,} rows")
            data_iterator = self._get_data_iterator()
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                list(executor.map(self._process_batch, data_iterator))
            
            self.metrics['processing_time'] = time() - start_time
            
            # Log final statistics
            self.logger.info("\nProcessing completed!")
            self.logger.info(f"Total rows processed: {self.metrics['total_rows']:,}")
            self.logger.info(f"Total batches processed: {self.metrics['batches_processed']:,}")
            self.logger.info(f"Total processing time: {self.metrics['processing_time']:.2f} seconds")
            self.logger.info(f"Average speed: {self.metrics['total_rows']/self.metrics['processing_time']:,.2f} rows/second")
            
        except Exception as e:
            self.logger.error(f"Data loading failed: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(
        description='Optimized NYC Trip Data Loader',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument('--user', required=True, help='PostgreSQL username')
    parser.add_argument('--password', required=True, help='PostgreSQL password')
    parser.add_argument('--host', required=True, help='Database host')
    parser.add_argument('--port', required=True, help='Database port')
    parser.add_argument('--db', required=True, help='Database name')
    parser.add_argument('--table-name', required=True, help='Destination table name')
    parser.add_argument('--url', required=True, help='URL of the data file')
    parser.add_argument('--batch-size', type=int, help='Batch size (auto-calculated if not specified)')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum number of parallel workers')
    
    args = parser.parse_args()
    
    try:
        loader = NYCTripDataLoader(
            url=args.url,
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port,
            db=args.db,
            table_name=args.table_name,
            batch_size=args.batch_size,
            max_workers=args.max_workers
        )
        
        loader.download_data()
        loader.load_and_insert_data()
        
    except Exception as e:
        logging.error(f"Process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()

