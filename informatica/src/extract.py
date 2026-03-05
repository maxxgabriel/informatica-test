"""
CSV File Extraction Module for Daily Sales Load
Handles reading CSV files with delimiter parsing and incremental filtering
"""

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
import yaml


class CSVExtractor:
    """Extract data from CSV files for staging tables"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize CSV extractor with configuration
        
        Args:
            config_path: Path to configuration file
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.source_path = self.config['parameters']['source_file_path']
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """Configure logging"""
        log_path = self.config['logging']['log_path']
        os.makedirs(log_path, exist_ok=True)
        
        logging.basicConfig(
            level=self.config['logging']['level'],
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"{log_path}/extract_{datetime.now().strftime('%Y%m%d')}.log"),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def extract_customer_data(self) -> pd.DataFrame:
        """
        Extract customer data from CSV file
        
        Returns:
            DataFrame with customer data
        """
        self.logger.info("Starting customer data extraction")
        
        file_config = self.config['source_files']['customer']
        file_path = os.path.join(self.source_path, file_config['name'])
        
        try:
            df = pd.read_csv(
                file_path,
                delimiter=file_config['delimiter'],
                header=0 if file_config['header'] else None
            )
            
            # Add metadata columns
            df['LOAD_DATE'] = datetime.now()
            df['SOURCE_SYSTEM'] = 'CSV_FILE'
            df['RECORD_ID'] = range(1, len(df) + 1)
            
            self.logger.info(f"Extracted {len(df)} customer records from {file_path}")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Customer file not found: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error extracting customer data: {str(e)}")
            raise
    
    def extract_product_data(self) -> pd.DataFrame:
        """
        Extract product data from CSV file
        
        Returns:
            DataFrame with product data
        """
        self.logger.info("Starting product data extraction")
        
        file_config = self.config['source_files']['product']
        file_path = os.path.join(self.source_path, file_config['name'])
        
        try:
            df = pd.read_csv(
                file_path,
                delimiter=file_config['delimiter'],
                header=0 if file_config['header'] else None
            )
            
            # Add metadata columns
            df['LOAD_DATE'] = datetime.now()
            df['SOURCE_SYSTEM'] = 'CSV_FILE'
            df['RECORD_ID'] = range(1, len(df) + 1)
            
            # Convert numeric columns
            numeric_cols = ['UNIT_PRICE', 'COST_PRICE', 'WEIGHT']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            self.logger.info(f"Extracted {len(df)} product records from {file_path}")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Product file not found: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error extracting product data: {str(e)}")
            raise
    
    def extract_sales_data(self) -> pd.DataFrame:
        """
        Extract sales transaction data from CSV files (supports wildcards)
        
        Returns:
            DataFrame with sales data
        """
        self.logger.info("Starting sales data extraction")
        
        file_config = self.config['source_files']['sales']
        pattern = file_config['pattern']
        
        try:
            # Find all matching files
            import glob
            file_pattern = os.path.join(self.source_path, file_config['name'])
            matching_files = glob.glob(file_pattern)
            
            if not matching_files:
                self.logger.warning(f"No sales files found matching pattern: {file_pattern}")
                return pd.DataFrame()
            
            # Read and combine all matching files
            dfs = []
            for file_path in matching_files:
                self.logger.info(f"Reading file: {file_path}")
                df = pd.read_csv(
                    file_path,
                    delimiter=file_config['delimiter'],
                    header=0 if file_config['header'] else None
                )
                dfs.append(df)
            
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Add metadata columns
            combined_df['LOAD_DATE'] = datetime.now()
            combined_df['SOURCE_SYSTEM'] = 'CSV_FILE'
            combined_df['RECORD_ID'] = range(1, len(combined_df) + 1)
            
            # Convert numeric columns
            numeric_cols = ['QUANTITY', 'UNIT_PRICE', 'DISCOUNT_PERCENT', 'TAX_AMOUNT', 'TOTAL_AMOUNT']
            for col in numeric_cols:
                if col in combined_df.columns:
                    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
            
            # Convert date columns
            if 'TRANSACTION_DATE' in combined_df.columns:
                combined_df['TRANSACTION_DATE'] = pd.to_datetime(combined_df['TRANSACTION_DATE'], errors='coerce')
            
            # Apply source filter (QUANTITY > 0 AND TOTAL_AMOUNT > 0)
            filtered_df = combined_df[
                (combined_df['QUANTITY'] > 0) & 
                (combined_df['TOTAL_AMOUNT'] > 0)
            ]
            
            self.logger.info(f"Extracted {len(filtered_df)} sales records from {len(matching_files)} files")
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Error extracting sales data: {str(e)}")
            raise
    
    def validate_extracted_data(self, df: pd.DataFrame, data_type: str) -> bool:
        """
        Validate extracted data for required columns and data quality
        
        Args:
            df: DataFrame to validate
            data_type: Type of data (customer, product, sales)
            
        Returns:
            True if validation passes
        """
        self.logger.info(f"Validating {data_type} data")
        
        if df.empty:
            self.logger.error(f"No data extracted for {data_type}")
            return False
        
        # Check for required metadata columns
        required_metadata = ['LOAD_DATE', 'SOURCE_SYSTEM', 'RECORD_ID']
        missing_cols = [col for col in required_metadata if col not in df.columns]
        
        if missing_cols:
            self.logger.error(f"Missing required metadata columns: {missing_cols}")
            return False
        
        # Data-specific validations
        if data_type == 'customer':
            required = ['CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME']
        elif data_type == 'product':
            required = ['PRODUCT_ID', 'PRODUCT_NAME', 'CATEGORY']
        elif data_type == 'sales':
            required = ['TRANSACTION_ID', 'CUSTOMER_ID', 'PRODUCT_ID', 'QUANTITY', 'TOTAL_AMOUNT']
        else:
            required = []
        
        missing_required = [col for col in required if col not in df.columns]
        if missing_required:
            self.logger.error(f"Missing required columns for {data_type}: {missing_required}")
            return False
        
        # Check for null values in key columns
        null_counts = df[required].isnull().sum()
        if null_counts.any():
            self.logger.warning(f"Null values found in key columns:\n{null_counts[null_counts > 0]}")
        
        self.logger.info(f"Validation passed for {data_type} data")
        return True
    
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """
        Extract all data sources
        
        Returns:
            Dictionary with extracted DataFrames
        """
        self.logger.info("Starting full data extraction")
        
        results = {}
        
        try:
            # Extract customer data
            customer_df = self.extract_customer_data()
            if self.validate_extracted_data(customer_df, 'customer'):
                results['customer'] = customer_df
            
            # Extract product data
            product_df = self.extract_product_data()
            if self.validate_extracted_data(product_df, 'product'):
                results['product'] = product_df
            
            # Extract sales data
            sales_df = self.extract_sales_data()
            if self.validate_extracted_data(sales_df, 'sales'):
                results['sales'] = sales_df
            
            self.logger.info(f"Extraction complete. Extracted {len(results)} data sources")
            return results
            
        except Exception as e:
            self.logger.error(f"Error in full extraction: {str(e)}")
            raise


def main():
    """Main execution function for testing"""
    extractor = CSVExtractor()
    data = extractor.extract_all()
    
    for source, df in data.items():
        print(f"\n{source.upper()} Data:")
        print(f"Records: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        print(df.head())


if __name__ == "__main__":
    main()