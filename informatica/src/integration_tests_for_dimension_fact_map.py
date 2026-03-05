===FILE: src/extract.py===
"""
Extract module for Sales DW Integration Tests
Reads data from CSV files and staging tables
"""
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import nipyapi
from nipyapi.nifi import ProcessorApi, ProcessGroupsApi

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extract data from CSV files for testing"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.source_path = Path(config.get('source_path', 'data/source'))
        
    def extract_customer_data(self, file_name: str = 'customer_master.csv') -> pd.DataFrame:
        """Extract customer data from CSV file"""
        try:
            file_path = self.source_path / file_name
            df = pd.read_csv(file_path)
            
            # Data validation
            required_columns = [
                'CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME', 'EMAIL',
                'PHONE', 'ADDRESS', 'CITY', 'STATE', 'ZIP_CODE', 
                'COUNTRY', 'REGISTRATION_DATE', 'CUSTOMER_TYPE'
            ]
            
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Add metadata
            df['LOAD_DATE'] = datetime.now()
            df['SOURCE_SYSTEM'] = 'CSV_FILE'
            
            logger.info(f"Extracted {len(df)} customer records from {file_name}")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting customer data: {str(e)}")
            raise
    
    def extract_product_data(self, file_name: str = 'product_master.csv') -> pd.DataFrame:
        """Extract product data from CSV file"""
        try:
            file_path = self.source_path / file_name
            df = pd.read_csv(file_path)
            
            required_columns = [
                'PRODUCT_ID', 'PRODUCT_NAME', 'PRODUCT_DESCRIPTION',
                'CATEGORY', 'SUB_CATEGORY', 'BRAND', 'UNIT_PRICE',
                'COST_PRICE', 'SUPPLIER_ID', 'SUPPLIER_NAME', 'STATUS'
            ]
            
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            df['LOAD_DATE'] = datetime.now()
            df['SOURCE_SYSTEM'] = 'CSV_FILE'
            
            logger.info(f"Extracted {len(df)} product records from {file_name}")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting product data: {str(e)}")
            raise
    
    def extract_sales_data(self, file_pattern: str = 'sales_transactions_*.csv') -> pd.DataFrame:
        """Extract sales data from CSV files"""
        try:
            sales_files = list(self.source_path.glob(file_pattern))
            
            if not sales_files:
                raise FileNotFoundError(f"No files found matching pattern: {file_pattern}")
            
            dfs = []
            for file_path in sales_files:
                df = pd.read_csv(file_path)
                dfs.append(df)
            
            df = pd.concat(dfs, ignore_index=True)
            
            required_columns = [
                'TRANSACTION_ID', 'TRANSACTION_DATE', 'CUSTOMER_ID',
                'PRODUCT_ID', 'QUANTITY', 'UNIT_PRICE', 'DISCOUNT_PERCENT',
                'TAX_AMOUNT', 'TOTAL_AMOUNT', 'PAYMENT_METHOD', 'STORE_ID', 'REGION'
            ]
            
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Filter valid records
            df = df[(df['QUANTITY'] > 0) & (df['TOTAL_AMOUNT'] > 0)]
            
            df['LOAD_DATE'] = datetime.now()
            df['SOURCE_SYSTEM'] = 'CSV_FILE'
            
            logger.info(f"Extracted {len(df)} sales records from {len(sales_files)} files")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting sales data: {str(e)}")
            raise
    
    def extract_staging_customer(self, last_extract_date: Optional[datetime] = None) -> pd.DataFrame:
        """Extract customer data from staging table (simulated)"""
        try:
            # In real implementation, this would query the database
            # For testing, we read from CSV and filter by date
            df = self.extract_customer_data()
            
            if last_extract_date:
                df = df[df['LOAD_DATE'] >= last_extract_date]
            
            logger.info(f"Extracted {len(df)} records from STG_CUSTOMER")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting from staging: {str(e)}")
            raise


class NiFiExtractor:
    """Extract data using NiFi processors"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.nifi_url = config.get('nifi_url', 'http://localhost:8080/nifi-api')
        self.root_pg_id = None
        
    def initialize_nifi(self):
        """Initialize NiFi connection"""
        try:
            nipyapi.config.nifi_config.host = self.nifi_url
            # Get root process group
            canvas = nipyapi.canvas.get_root_pg_id()
            self.root_pg_id = canvas
            logger.info(f"Connected to NiFi at {self.nifi_url}")
            
        except Exception as e:
            logger.error(f"Failed to connect to NiFi: {str(e)}")
            raise
    
    def create_extract_process_group(self, pg_name: str = 'Sales_Extract') -> str:
        """Create process group for data extraction"""
        try:
            pg = nipyapi.canvas.create_process_group(
                parent_pg=nipyapi.canvas.get_process_group(self.root_pg_id, 'id'),
                name=pg_name,
                location=(100, 100)
            )
            logger.info(f"Created process group: {pg_name}")
            return pg.id
            
        except Exception as e:
            logger.error(f"Error creating process group: {str(e)}")
            raise
    
    def create_file_reader_processor(
        self, 
        parent_pg_id: str,
        processor_name: str,
        input_directory: str,
        file_filter: str
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create GetFile processor for reading CSV files"""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(parent_pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('GetFile'),
                location=(200, 200),
                name=processor_name
            )
            
            # Configure processor properties
            nipyapi.canvas.update_processor(
                processor,
                nipyapi.nifi.ProcessorConfigDTO(
                    properties={
                        'Input Directory': input_directory,
                        'File Filter': file_filter,
                        'Keep Source File': 'false',
                        'Batch Size': '10',
                        'Polling Interval': '10 sec'
                    }
                )
            )
            
            logger.info(f"Created GetFile processor: {processor_name}")
            return processor
            
        except Exception as e:
            logger.error(f"Error creating file reader: {str(e)}")
            raise


===FILE: src/transform.py===
"""
Transform module for Sales DW Integration Tests
Implements cleansing, SCD logic, and business rules
"""
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class CustomerDimensionTransformer:
    """Transform customer data with SCD Type 2 logic"""
    
    def __init__(self, config: Dict):
        self.config = config
        
    def cleanse_customer_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleanse and standardize customer data"""
        try:
            df_clean = df.copy()
            
            # Clean text fields
            df_clean['FIRST_NAME_CLEAN'] = df_clean['FIRST_NAME'].str.strip().str.upper()
            df_clean['LAST_NAME_CLEAN'] = df_clean['LAST_NAME'].str.strip().str.upper()
            df_clean['FULL_NAME'] = df_clean['FIRST_NAME_CLEAN'] + ' ' + df_clean['LAST_NAME_CLEAN']
            
            # Clean email
            df_clean['EMAIL_CLEAN'] = df_clean['EMAIL'].str.strip().str.lower()
            
            # Clean phone - remove special characters
            df_clean['PHONE_CLEAN'] = df_clean['PHONE'].str.replace(r'[()-\s]', '', regex=True)
            
            # Clean address fields
            df_clean['ADDRESS_CLEAN'] = df_clean['ADDRESS'].str.strip().str.title()
            df_clean['CITY_CLEAN'] = df_clean['CITY'].str.strip().str.upper()
            df_clean['STATE_CLEAN'] = df_clean['STATE'].str.strip().str.upper()
            df_clean['ZIP_CODE_CLEAN'] = df_clean['ZIP_CODE'].str.strip()
            df_clean['COUNTRY_CLEAN'] = df_clean['COUNTRY'].str.strip().str.upper()
            
            # Clean customer type
            df_clean['CUSTOMER_TYPE_CLEAN'] = df_clean['CUSTOMER_TYPE'].str.strip().str.upper()
            
            df_clean['CURRENT_TIMESTAMP'] = datetime.now()
            
            logger.info(f"Cleansed {len(df_clean)} customer records")
            return df_clean
            
        except Exception as e:
            logger.error(f"Error cleansing customer data: {str(e)}")
            raise
    
    def apply_scd_type2_logic(
        self, 
        staging_df: pd.DataFrame, 
        dimension_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Apply SCD Type 2 logic
        Returns: (new_records, changed_records, unchanged_records)
        """
        try:
            # Get current records from dimension
            current_dim = dimension_df[dimension_df['IS_CURRENT'] == 'Y'].copy()
            
            # Merge to identify new and changed records
            merged = staging_df.merge(
                current_dim,
                on='CUSTOMER_ID',
                how='left',
                suffixes=('', '_DIM')
            )
            
            # Identify new records
            new_records = merged[merged['CUSTOMER_KEY'].isna()].copy()
            
            # Identify changed records
            existing = merged[merged['CUSTOMER_KEY'].notna()].copy()
            
            # Check for changes in tracked columns
            change_columns = [
                'FIRST_NAME_CLEAN', 'LAST_NAME_CLEAN', 'EMAIL_CLEAN',
                'PHONE_CLEAN', 'ADDRESS_CLEAN'
            ]
            
            changed_mask = pd.Series([False] * len(existing))
            for col in change_columns:
                if col in existing.columns and f'{col}_DIM' in existing.columns:
                    changed_mask |= (existing[col] != existing[f'{col}_DIM'])
            
            changed_records = existing[changed_mask].copy()
            unchanged_records = existing[~changed_mask].copy()
            
            # Assign surrogate keys for new records
            max_key = dimension_df['CUSTOMER_KEY'].max() if len(dimension_df) > 0 else 0
            new_records['CUSTOMER_KEY'] = range(max_key + 1, max_key + 1 + len(new_records))
            
            # Assign new surrogate keys for changed records
            changed_records['CUSTOMER_KEY'] = range(
                max_key + len(new_records) + 1,
                max_key + len(new_records) + 1 + len(changed_records)
            )
            
            # Set SCD dates
            current_date = datetime.now()
            
            for df_subset in [new_records, changed_records]:
                df_subset['EFFECTIVE_FROM_DATE'] = current_date
                df_subset['EFFECTIVE_TO_DATE'] = pd.to_datetime('9999-12-31')
                df_subset['IS_CURRENT'] = 'Y'
                df_subset['CREATED_DATE'] = current_date
            
            logger.info(
                f"SCD Type 2 logic: {len(new_records)} new, "
                f"{len(changed_records)} changed, {len(unchanged_records)} unchanged"
            )
            
            return new_records, changed_records, unchanged_records
            
        except Exception as e:
            logger.error(f"Error applying SCD Type 2 logic: {str(e)}")
            raise
    
    def prepare_expired_records(
        self, 
        changed_records: pd.DataFrame,
        dimension_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Prepare old versions to be expired"""
        try:
            customer_ids_to_expire = changed_records['CUSTOMER_ID'].unique()
            
            to_expire = dimension_df[
                (dimension_df['CUSTOMER_ID'].isin(customer_ids_to_expire)) &
                (dimension_df['IS_CURRENT'] == 'Y')
            ].copy()
            
            to_expire['EFFECTIVE_TO_DATE'] = datetime.now() - timedelta(days=1)
            to_expire['IS_CURRENT'] = 'N'
            to_expire['UPDATED_DATE'] = datetime.now()
            
            logger.info(f"Prepared {len(to_expire)} records for expiration")
            return to_expire
            
        except Exception as e:
            logger.error(f"Error preparing expired records: {str(e)}")
            raise


class ProductDimensionTransformer:
    """Transform product data with SCD Type 1 logic"""
    
    def __init__(self, config: Dict):
        self.config = config
    
    def cleanse_product_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleanse and standardize product data"""
        try:
            df_clean = df.copy()
            
            # Clean product fields
            df_clean['PRODUCT_NAME_CLEAN'] = df_clean['PRODUCT_NAME'].str.strip().str.title()
            df_clean['PRODUCT_DESC_CLEAN'] = df_clean['PRODUCT_DESCRIPTION'].str.strip()
            df_clean['CATEGORY_CLEAN'] = df_clean['CATEGORY'].str.strip().str.upper()
            df_clean['SUB_CATEGORY_CLEAN'] = df_clean['SUB_CATEGORY'].str.strip().str.upper()
            df_clean['BRAND_CLEAN'] = df_clean['BRAND'].str.strip().str.title()
            
            # Clean prices
            df_clean['UNIT_PRICE_CLEAN'] = df_clean['UNIT_PRICE'].round(2)
            df_clean['COST_PRICE_CLEAN'] = df_clean['COST_PRICE'].round(2)
            
            # Clean supplier
            df_clean['SUPPLIER_NAME_CLEAN'] = df_clean['SUPPLIER_NAME'].str.strip().str.title()
            df_clean['STATUS_CLEAN'] = df_clean['STATUS'].str.strip().str.upper()
            
            # Calculate profit margin
            df_clean['PROFIT_MARGIN'] = np.where(
                df_clean['UNIT_PRICE_CLEAN'] > 0,
                ((df_clean['UNIT_PRICE_CLEAN'] - df_clean['COST_PRICE_CLEAN']) / 
                 df_clean['UNIT_PRICE_CLEAN'] * 100).round(2),
                0
            )
            
            # Determine price range
            df_clean['PRICE_RANGE'] = pd.cut(
                df_clean['UNIT_PRICE_CLEAN'],
                bins=[0, 50, 200, 500, float('inf')],
                labels=['LOW', 'MEDIUM', 'HIGH', 'PREMIUM']
            )
            
            df_clean['CURRENT_TIMESTAMP'] = datetime.now()
            
            logger.info(f"Cleansed {len(df_clean)} product records")
            return df_clean
            
        except Exception as e:
            logger.error(f"Error cleansing product data: {str(e)}")
            raise
    
    def apply_scd_type1_logic(
        self,
        staging_df: pd.DataFrame,
        dimension_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Apply SCD Type 1 logic (overwrite)
        Returns: (new_records, updated_records, unchanged_records)
        """
        try:
            # Merge with existing dimension
            merged = staging_df.merge(
                dimension_df,
                on='PRODUCT_ID',
                how='left',
                suffixes=('', '_DIM')
            )
            
            # Identify new records
            new_records = merged[merged['PRODUCT_KEY'].isna()].copy()
            
            # Identify existing records
            existing = merged[merged['PRODUCT_KEY'].notna()].copy()
            
            # Check for changes in tracked columns
            change_columns = [
                'PRODUCT_NAME_CLEAN', 'CATEGORY_CLEAN',
                'UNIT_PRICE_CLEAN', 'STATUS_CLEAN'
            ]
            
            changed_mask = pd.Series([False] * len(existing))
            for col in change_columns:
                if col in existing.columns and f'{col}_DIM' in existing.columns:
                    changed_mask |= (existing[col] != existing[f'{col}_DIM'])
            
            updated_records = existing[changed_mask].copy()
            unchanged_records = existing[~changed_mask].copy()
            
            # Assign surrogate keys for new records
            max_key = dimension_df['PRODUCT_KEY'].max() if len(dimension_df) > 0 else 0
            new_records['PRODUCT_KEY'] = range(max_key + 1, max_key + 1 + len(new_records))
            new_records['CREATED_DATE'] = datetime.now()
            
            # Keep existing keys for updated records
            updated_records['UPDATED_DATE'] = datetime.now()
            
            logger.info(
                f"SCD Type 1 logic: {len(new_records)} new, "
                f"{len(updated_records)} updated, {len(unchanged_records)} unchanged"
            )
            
            return new_records, updated_records, unchanged_records
            
        except Exception as e:
            logger.error(f"Error applying SCD Type 1 logic: {str(e)}")
            raise
    
    def filter_valid_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out invalid product records"""
        try:
            valid_df = df[
                df['PRODUCT_ID'].notna() &
                df['PRODUCT_NAME_CLEAN'].notna() &
                df['CATEGORY_CLEAN'].notna() &
                (df['UNIT_PRICE_CLEAN'] >= 0) &
                (df['COST_PRICE_CLEAN'] >= 0)
            ].copy()
            
            invalid_count = len(df) - len(valid_df)
            if invalid_count > 0:
                logger.warning(f"Filtered out {invalid_count} invalid product records")
            
            return valid_df
            
        except Exception as e:
            logger.error(f"Error filtering products: {str(e)}")
            raise


class SalesFactTransformer:
    """Transform sales data for fact table"""
    
    def __init__(self, config: Dict):
        self.config = config
    
    def calculate_date_key(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate date key from transaction date"""
        try:
            df_out = df.copy()
            df_out['TRANSACTION_DATE'] = pd.to_datetime(df_out['TRANSACTION_DATE'])
            df_out['DATE_KEY'] = df_out['TRANSACTION_DATE'].dt.strftime('%Y%m%d').astype(int)
            df_out['TRANSACTION_DATE_ONLY'] = df_out['TRANSACTION_DATE'].dt.date
            
            logger.info(f"Calculated date keys for {len(df_out)} records")
            return df_out
            
        except Exception as e:
            logger.error(f"Error calculating date key: {str(e)}")
            raise
    
    def lookup_dimension_keys(
        self,
        sales_df: pd.DataFrame,
        customer_dim: pd.DataFrame,
        product_dim: pd.DataFrame
    ) -> pd.DataFrame:
        """Lookup surrogate keys from dimension tables"""
        try:
            df_out = sales_df.copy()
            
            # Lookup customer key (with SCD Type 2 date check)
            current_customers = customer_dim[customer_dim['IS_CURRENT'] == 'Y'].copy()
            
            df_out = df_out.merge(
                current_customers[['CUSTOMER_ID', 'CUSTOMER_KEY']],
                on='CUSTOMER_ID',
                how='left'
            )
            
            # Lookup product key
            df_out = df_out.merge(
                product_dim[['PRODUCT_ID', 'PRODUCT_KEY', 'COST_PRICE']],
                on='PRODUCT_ID',
                how='left',
                suffixes=('', '_PROD')
            )
            
            # Log lookup statistics
            missing_customers = df_out['CUSTOMER_KEY'].isna().sum()
            missing_products = df_out['PRODUCT_KEY'].isna().sum()
            
            if missing_customers > 0:
                logger.warning(f"Could not find {missing_customers} customer keys")
            if missing_products > 0:
                logger.warning(f"Could not find {missing_products} product keys")
            
            logger.info(f"Looked up dimension keys for {len(df_out)} records")
            return df_out
            
        except Exception as e:
            logger.error(f"Error looking up dimension keys: {str(e)}")
            raise
    
    def calculate_measures(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate business metrics"""
        try:
            df_out = df.copy()
            
            # Calculate discount amount
            df_out['DISCOUNT_AMOUNT'] = (
                df_out['UNIT_PRICE'] * df_out['QUANTITY'] * 
                df_out['DISCOUNT_PERCENT'] / 100
            ).round(2)
            
            # Calculate cost amount
            df_out['COST_AMOUNT'] = (
                df_out['COST_PRICE'] * df_out['QUANTITY']
            ).round(2)
            
            # Calculate profit
            df_out['PROFIT_AMOUNT'] = (
                df_out['TOTAL_AMOUNT'] - df_out['TAX_AMOUNT'] - df_out['COST_AMOUNT']
            ).round(2)
            
            # Calculate profit margin
            net_sales = df_out['TOTAL_AMOUNT'] - df_out['TAX_AMOUNT']
            df_out['PROFIT_MARGIN_PERCENT'] = np.where(
                net_sales > 0,
                (df_out['PROFIT_AMOUNT'] / net_sales * 100).round(2),
                0
            )
            
            # Generate surrogate key
            df_out['SALES_KEY'] = range(1, len(df_out) + 1)
            df_out['LOAD_TIMESTAMP'] = datetime.now()
            
            logger.info(f"Calculated measures for {len(df_out)} sales records")
            return df_out
            
        except Exception as e:
            logger.error(f"Error calculating measures: {str(e)}")
            raise
    
    def filter_valid_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out invalid transactions"""
        try:
            valid_df = df[
                df['CUSTOMER_KEY'].notna() &
                df['PRODUCT_KEY'].notna() &
                (df['QUANTITY'] > 0) &
                (df['TOTAL_AMOUNT'] > 0)
            ].copy()
            
            invalid_count = len(df) - len(valid_df)
            if invalid_count > 0:
                logger.warning(f"Filtered out {invalid_count} invalid transactions")
            
            return valid_df
            
        except Exception as e:
            logger.error(f"Error filtering transactions: {str(e)}")
            raise
    
    def deduplicate_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate transactions"""
        try:
            df_dedup = df.drop_duplicates(subset=['TRANSACTION_ID'], keep='first')
            
            dup_count = len(df) - len(df_dedup)
            if dup_count > 0:
                logger.warning(f"Removed {dup_count} duplicate transactions")
            
            return df_dedup
            
        except Exception as e:
            logger.error(f"Error deduplicating: {str(e)}")
            raise


===FILE: src/load.py===
"""
Load module for Sales DW Integration Tests
Handles loading to dimension and fact tables
"""
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional
import nipyapi
from nipyapi.nifi import ProcessorApi

logger = logging.getLogger(__name__)


class DimensionLoader:
    """Load data to dimension tables"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.connection_config = config.get('database', {})
    
    def load_customer_dimension(
        self,
        new_records: pd.DataFrame,
        changed_records: pd.DataFrame,
        expired_records: pd.DataFrame
    ) -> Dict[str, int]:
        """
        Load customer dimension with SCD Type 2
        Returns statistics dict
        """
        try:
            stats = {
                'new_inserted': 0,
                'changed_inserted': 0,
                'expired_updated': 0,
                'total_processed': 0
            }
            
            # In real implementation, use database connection
            # This is a simulation for testing
            
            # Insert new records
            if len(new_records) > 0:
                # INSERT INTO DIM_CUSTOMER
                stats['new_inserted'] = len(new_records)
                logger.info(f"Inserted {stats['new_inserted']} new customer records")
            
            # Insert changed records (new versions)
            if len(changed_records) > 0:
                # INSERT INTO DIM_CUSTOMER
                stats['changed_inserted'] = len(changed_records)
                logger.info(f"Inserted {stats['changed_inserted']} changed customer versions")
            
            # Update expired records
            if len(expired_records) > 0:
                # UPDATE DIM_CUSTOMER SET IS_CURRENT = 'N', EFFECTIVE_TO_DATE = ...
                stats['expired_updated'] = len(expired_records)
                logger.info(f"Expired {stats['expired_updated']} old customer versions")
            
            stats['total_processed'] = sum([
                stats['new_inserted'],
                stats['changed_inserted'],
                stats['expired_updated']
            ])
            
            return stats
            
        except Exception as e:
            logger.error(f"Error loading customer dimension: {str(e)}")
            raise
    
    def load_product_dimension(
        self,
        new_records: pd.DataFrame,
        updated_records: pd.DataFrame
    ) -> Dict[str, int]:
        """
        Load product dimension with SCD Type 1
        Returns statistics dict
        """
        try:
            stats = {
                'new_inserted': 0,
                'updated': 0,
                'total_processed': 0
            }
            
            # Insert new records
            if len(new_records) > 0:
                # INSERT INTO DIM_PRODUCT
                stats['new_inserted'] = len(new_records)
                logger.info(f"Inserted {stats['new_inserted']} new product records")
            
            # Update existing records
            if len(updated_records) > 0:
                # UPDATE DIM_PRODUCT
                stats['updated'] = len(updated_records)
                logger.info(f"Updated {stats['updated']} product records")
            
            stats['total_processed'] = stats['new_inserted'] + stats['updated']
            
            return stats
            
        except Exception as e:
            logger.error(f"Error loading product dimension: {str(e)}")
            raise


class FactLoader:
    """Load data to fact tables"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.connection_config = config.get('database', {})
        self.batch_size = config.get('batch_size', 10000)
    
    def load_sales_fact(self, sales_df: pd.DataFrame) -> Dict[str, int]:
        """
        Load sales fact table
        Returns statistics dict
        """
        try:
            stats = {
                'records_inserted': 0,
                'batches_processed': 0,
                'errors': 0
            }
            
            # Process in batches for better performance
            total_batches = (len(sales_df) + self.batch_size - 1) // self.batch_size
            
            for i in range(0, len(sales_df), self.batch_size):
                batch = sales_df.iloc[i:i + self.batch_size]
                
                try:
                    # In real implementation: INSERT INTO FACT_SALES
                    stats['records_inserted'] += len(batch)
                    stats['batches_processed'] += 1
                    
                    if stats['batches_processed'] % 10 == 0:
                        logger.info(
                            f"Progress: {stats['batches_processed']}/{total_batches} batches, "
                            f"{stats['records_inserted']} records loaded"
                        )
                        
                except Exception as batch_error:
                    logger.error(f"Error loading batch {stats['batches_processed']}: {str(batch_error)}")
                    stats['errors'] += len(batch)
            
            logger.info(
                f"Loaded {stats['records_inserted']} sales records in "
                f"{stats['batches_processed']} batches with {stats['errors']} errors"
            )
            
            return stats
            
        except Exception as e:
            logger.error(f"Error loading sales fact: {str(e)}")
            raise
    
    def validate_referential_integrity(
        self,
        sales_df: pd.DataFrame,
        customer_dim: pd.DataFrame,
        product_dim: pd.DataFrame
    ) -> Dict[str, any]:
        """Validate referential integrity before loading"""
        try:
            validation_results = {
                'valid': True,
                'missing_customers': [],
                'missing_products': [],
                'total_violations': 0
            }
            
            # Check customer keys
            valid_customer_keys = set(customer_dim