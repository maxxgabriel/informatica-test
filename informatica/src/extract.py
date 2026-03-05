"""
Extract module for Full Refresh workflow
Loads CSV files into staging tables with full truncate/reload
"""

import pandas as pd
import logging
from typing import Dict, Any, List
from pathlib import Path
import glob
from datetime import datetime

from src.database_utils import DatabaseManager

logger = logging.getLogger(__name__)


class StagingExtractor:
    """Extracts data from source files and loads to staging tables"""
    
    def __init__(self, config: Dict[str, Any], db_manager: DatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.source_path = Path(config['source_files']['base_path'])
        self.archive_path = Path(config['source_files']['archive_path'])
        
    def extract_customer_data(self) -> pd.DataFrame:
        """Extract customer data from CSV"""
        file_name = self.config['source_files']['customer']
        file_path = self.source_path / file_name
        
        logger.info(f"Extracting customer data from {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            
            # Add metadata columns
            df['LOAD_DATE'] = datetime.now()
            df['SOURCE_SYSTEM'] = 'CSV_FILE'
            df['RECORD_ID'] = range(1, len(df) + 1)
            
            logger.info(f"Extracted {len(df)} customer records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting customer data: {e}")
            raise
    
    def extract_product_data(self) -> pd.DataFrame:
        """Extract product data from CSV"""
        file_name = self.config['source_files']['product']
        file_path = self.source_path / file_name
        
        logger.info(f"Extracting product data from {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            
            # Add metadata columns
            df['LOAD_DATE'] = datetime.now()
            df['SOURCE_SYSTEM'] = 'CSV_FILE'
            df['RECORD_ID'] = range(1, len(df) + 1)
            
            logger.info(f"Extracted {len(df)} product records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting product data: {e}")
            raise
    
    def extract_sales_data(self) -> pd.DataFrame:
        """Extract sales data from multiple CSV files"""
        file_pattern = self.config['source_files']['sales']
        file_paths = glob.glob(str(self.source_path / file_pattern))
        
        logger.info(f"Extracting sales data from {len(file_paths)} files")
        
        if not file_paths:
            raise FileNotFoundError(f"No files found matching pattern: {file_pattern}")
        
        try:
            # Read and combine all sales files
            dfs = []
            for file_path in file_paths:
                df = pd.read_csv(file_path)
                dfs.append(df)
                logger.info(f"Read {len(df)} records from {file_path}")
            
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Add metadata columns
            combined_df['LOAD_DATE'] = datetime.now()
            combined_df['SOURCE_SYSTEM'] = 'CSV_FILE'
            combined_df['RECORD_ID'] = range(1, len(combined_df) + 1)
            
            # Filter valid records
            combined_df = combined_df[
                (combined_df['QUANTITY'] > 0) & 
                (combined_df['TOTAL_AMOUNT'] > 0)
            ]
            
            logger.info(f"Extracted {len(combined_df)} total sales records")
            return combined_df
            
        except Exception as e:
            logger.error(f"Error extracting sales data: {e}")
            raise
    
    def load_to_staging(self, df: pd.DataFrame, table_name: str) -> int:
        """
        Load dataframe to staging table with truncate/reload
        
        Args:
            df: DataFrame to load
            table_name: Target staging table name
            
        Returns:
            Number of rows loaded
        """
        logger.info(f"Loading {len(df)} records to {table_name}")
        
        try:
            # Truncate staging table first
            self.db_manager.truncate_table(table_name, warehouse=False)
            
            # Bulk load data
            with self.db_manager.get_staging_connection() as conn:
                # Use copy_from for efficient bulk loading
                df.to_sql(
                    table_name.lower(),
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=self.config['execution']['bulk_load_batch_size']
                )
            
            logger.info(f"Successfully loaded {len(df)} records to {table_name}")
            return len(df)
            
        except Exception as e:
            logger.error(f"Error loading to {table_name}: {e}")
            raise
    
    def archive_source_files(self) -> None:
        """Archive processed source files"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_subdir = self.archive_path / timestamp
        archive_subdir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Archiving source files to {archive_subdir}")
        
        try:
            # Archive customer file
            customer_file = self.source_path / self.config['source_files']['customer']
            if customer_file.exists():
                customer_file.rename(archive_subdir / customer_file.name)
            
            # Archive product file
            product_file = self.source_path / self.config['source_files']['product']
            if product_file.exists():
                product_file.rename(archive_subdir / product_file.name)
            
            # Archive sales files
            sales_pattern = self.config['source_files']['sales']
            sales_files = glob.glob(str(self.source_path / sales_pattern))
            for sales_file in sales_files:
                Path(sales_file).rename(archive_subdir / Path(sales_file).name)
            
            logger.info(f"Archived {len(sales_files) + 2} files")
            
        except Exception as e:
            logger.error(f"Error archiving files: {e}")
            raise
    
    def run_full_staging_load(self) -> Dict[str, int]:
        """
        Execute full staging load process
        
        Returns:
            Dictionary with row counts per table
        """
        logger.info("Starting full staging load")
        results = {}
        
        try:
            # Extract and load customer data
            customer_df = self.extract_customer_data()
            customer_count = self.load_to_staging(
                customer_df, 
                self.config['staging_tables']['customer']
            )
            results['customer'] = customer_count
            
            # Extract and load product data
            product_df = self.extract_product_data()
            product_count = self.load_to_staging(
                product_df,
                self.config['staging_tables']['product']
            )
            results['product'] = product_count
            
            # Extract and load sales data
            sales_df = self.extract_sales_data()
            sales_count = self.load_to_staging(
                sales_df,
                self.config['staging_tables']['sales']
            )
            results['sales'] = sales_count
            
            # Archive source files
            self.archive_source_files()
            
            logger.info(f"Staging load complete: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Staging load failed: {e}")
            raise