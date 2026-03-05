"""
Staging Loader Module
Handles full truncate and reload of staging tables from CSV files
"""

import logging
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class StagingLoader:
    """Load CSV files into staging tables with full refresh"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.db_config = config['database']['staging']
        self.source_config = config['source']
        self.staging_config = config['staging']
        self.high_precision = config['processing']['high_precision_numeric']
        
    @contextmanager
    def get_connection(self):
        """Database connection context manager"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.db_config['connection_url'].split('//')[1].split(':')[0],
                database=self.db_config['connection_url'].split('/')[-1],
                user=self.db_config['username'],
                password=self.db_config['password']
            )
            conn.autocommit = False
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def truncate_staging_table(self, conn, table_name: str):
        """Truncate staging table before load"""
        try:
            cursor = conn.cursor()
            logger.info(f"Truncating staging table: {table_name}")
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()
            logger.info(f"Successfully truncated {table_name}")
        except Exception as e:
            logger.error(f"Error truncating {table_name}: {e}")
            raise
    
    def convert_high_precision_numeric(self, value: Any, precision: int, scale: int) -> Optional[Decimal]:
        """Convert to high precision decimal"""
        if pd.isna(value) or value is None:
            return None
        try:
            dec_value = Decimal(str(value))
            quantize_str = '0.' + '0' * scale
            return dec_value.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
        except Exception as e:
            logger.warning(f"Error converting {value} to Decimal: {e}")
            return None
    
    def clean_string(self, value: Any, operation: str = 'trim') -> Optional[str]:
        """Clean string values"""
        if pd.isna(value) or value is None:
            return None
        
        str_value = str(value).strip()
        
        if operation == 'upper':
            return str_value.upper()
        elif operation == 'lower':
            return str_value.lower()
        elif operation == 'initcap':
            return str_value.title()
        else:
            return str_value
    
    def clean_phone(self, phone: str) -> Optional[str]:
        """Remove phone number formatting"""
        if pd.isna(phone) or phone is None:
            return None
        return ''.join(c for c in str(phone) if c.isdigit())
    
    def load_customer_staging(self, file_path: str) -> int:
        """Load customer master CSV to staging table"""
        logger.info(f"Loading customer data from {file_path}")
        
        try:
            # Read CSV with explicit dtype for precision
            df = pd.read_csv(
                file_path,
                delimiter=self.source_config['delimiter'],
                encoding=self.source_config['encoding'],
                dtype={
                    'CUSTOMER_ID': str,
                    'FIRST_NAME': str,
                    'LAST_NAME': str,
                    'EMAIL': str,
                    'PHONE': str,
                    'ADDRESS': str,
                    'CITY': str,
                    'STATE': str,
                    'ZIP_CODE': str,
                    'COUNTRY': str,
                    'CUSTOMER_TYPE': str
                },
                parse_dates=['REGISTRATION_DATE']
            )
            
            logger.info(f"Read {len(df)} records from CSV")
            
            # Data cleansing
            df['FIRST_NAME_CLEAN'] = df['FIRST_NAME'].apply(lambda x: self.clean_string(x, 'upper'))
            df['LAST_NAME_CLEAN'] = df['LAST_NAME'].apply(lambda x: self.clean_string(x, 'upper'))
            df['FULL_NAME'] = df['FIRST_NAME_CLEAN'] + ' ' + df['LAST_NAME_CLEAN']
            df['EMAIL_CLEAN'] = df['EMAIL'].apply(lambda x: self.clean_string(x, 'lower'))
            df['PHONE_CLEAN'] = df['PHONE'].apply(self.clean_phone)
            df['ADDRESS_CLEAN'] = df['ADDRESS'].apply(lambda x: self.clean_string(x, 'initcap'))
            df['CITY_CLEAN'] = df['CITY'].apply(lambda x: self.clean_string(x, 'upper'))
            df['STATE_CLEAN'] = df['STATE'].apply(lambda x: self.clean_string(x, 'upper'))
            df['ZIP_CODE_CLEAN'] = df['ZIP_CODE'].apply(lambda x: self.clean_string(x, 'trim'))
            df['COUNTRY_CLEAN'] = df['COUNTRY'].apply(lambda x: self.clean_string(x, 'upper'))
            df['CUSTOMER_TYPE_CLEAN'] = df['CUSTOMER_TYPE'].apply(lambda x: self.clean_string(x, 'upper'))
            df['LOAD_DATE'] = datetime.now()
            df['SOURCE_SYSTEM'] = 'CSV_FILE'
            
            with self.get_connection() as conn:
                # Truncate if configured
                if self.staging_config['truncate_before_load']:
                    self.truncate_staging_table(conn, self.staging_config['customer_table'])
                
                cursor = conn.cursor()
                
                # Prepare insert statement
                insert_sql = f"""
                    INSERT INTO {self.staging_config['customer_table']} (
                        CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
                        ADDRESS, CITY, STATE, ZIP_CODE, COUNTRY,
                        REGISTRATION_DATE, CUSTOMER_TYPE, LOAD_DATE, SOURCE_SYSTEM
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                
                # Prepare data for batch insert
                records = []
                for _, row in df.iterrows():
                    records.append((
                        row['CUSTOMER_ID'],
                        row['FIRST_NAME_CLEAN'],
                        row['LAST_NAME_CLEAN'],
                        row['EMAIL_CLEAN'],
                        row['PHONE_CLEAN'],
                        row['ADDRESS_CLEAN'],
                        row['CITY_CLEAN'],
                        row['STATE_CLEAN'],
                        row['ZIP_CODE_CLEAN'],
                        row['COUNTRY_CLEAN'],
                        row['REGISTRATION_DATE'],
                        row['CUSTOMER_TYPE_CLEAN'],
                        row['LOAD_DATE'],
                        row['SOURCE_SYSTEM']
                    ))
                
                # Execute batch insert
                execute_batch(
                    cursor,
                    insert_sql,
                    records,
                    page_size=self.staging_config['batch_size']
                )
                
                conn.commit()
                logger.info(f"Successfully loaded {len(records)} customer records to staging")
                return len(records)
                
        except Exception as e:
            logger.error(f"Error loading customer staging: {e}")
            raise
    
    def load_product_staging(self, file_path: str) -> int:
        """Load product master CSV to staging table"""
        logger.info(f"Loading product data from {file_path}")
        
        try:
            # Read CSV with high precision numeric handling
            df = pd.read_csv(
                file_path,
                delimiter=self.source_config['delimiter'],
                encoding=self.source_config['encoding'],
                dtype={
                    'PRODUCT_ID': str,
                    'PRODUCT_NAME': str,
                    'PRODUCT_DESCRIPTION': str,
                    'CATEGORY': str,
                    'SUB_CATEGORY': str,
                    'BRAND': str,
                    'SUPPLIER_ID': str,
                    'SUPPLIER_NAME': str,
                    'DIMENSIONS': str,
                    'COLOR': str,
                    'SIZE': str,
                    'MATERIAL': str,
                    'STATUS': str
                }
            )
            
            logger.info(f"Read {len(df)} records from CSV")
            
            # Data cleansing
            df['PRODUCT_NAME_CLEAN'] = df['PRODUCT_NAME'].apply(lambda x: self.clean_string(x, 'initcap'))
            df['PRODUCT_DESC_CLEAN'] = df['PRODUCT_DESCRIPTION'].apply(lambda x: self.clean_string(x, 'trim'))
            df['CATEGORY_CLEAN'] = df['CATEGORY'].apply(lambda x: self.clean_string(x, 'upper'))
            df['SUB_CATEGORY_CLEAN'] = df['SUB_CATEGORY'].apply(lambda x: self.clean_string(x, 'upper'))
            df['BRAND_CLEAN'] = df['BRAND'].apply(lambda x: self.clean_string(x, 'initcap'))
            df['SUPPLIER_NAME_CLEAN'] = df['SUPPLIER_NAME'].apply(lambda x: self.clean_string(x, 'initcap'))
            df['STATUS_CLEAN'] = df['STATUS'].apply(lambda x: self.clean_string(x, 'upper'))
            
            # High precision numeric conversion
            numeric_precision = self.config['processing']['numeric_precision']
            numeric_scale = self.config['processing']['numeric_scale']
            
            df['UNIT_PRICE_CLEAN'] = df['UNIT_PRICE'].apply(
                lambda x: self.convert_high_precision_numeric(x, numeric_scale, numeric_precision)
            )
            df['COST_PRICE_CLEAN'] = df['COST_PRICE'].apply(
                lambda x: self.convert_high_precision_numeric(x, numeric_scale, numeric_precision)
            )
            df['WEIGHT_CLEAN'] = df['WEIGHT'].apply(
                lambda x: self.convert_high_precision_numeric(x, numeric_scale, numeric_precision)
            )
            
            # Calculate derived fields
            df['PROFIT_MARGIN'] = df.apply(
                lambda row: round(
                    ((row['UNIT_PRICE_CLEAN'] - row['COST_PRICE_CLEAN']) / row['UNIT_PRICE_CLEAN'] * 100)
                    if row['UNIT_PRICE_CLEAN'] and row['UNIT_PRICE_CLEAN'] > 0 else 0,
                    2
                ),
                axis=1
            )
            
            def get_price_range(price):
                if pd.isna(price) or price is None:
                    return 'UNKNOWN'
                price_val = float(price)
                if price_val < 50:
                    return 'LOW'
                elif price_val < 200:
                    return 'MEDIUM'
                elif price_val < 500:
                    return 'HIGH'
                else:
                    return 'PREMIUM'
            
            df['PRICE_RANGE'] = df['UNIT_PRICE_CLEAN'].apply(get_price_range)
            df['LOAD_DATE'] = datetime.now()
            df['SOURCE_SYSTEM'] = 'CSV_FILE'
            
            with self.get_connection() as conn:
                # Truncate if configured
                if self.staging_config['truncate_before_load']:
                    self.truncate_staging_table(conn, self.staging_config['product_table'])
                
                cursor = conn.cursor()
                
                # Prepare insert statement
                insert_sql = f"""
                    INSERT INTO {self.staging_config['product_table']} (
                        PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCRIPTION,
                        CATEGORY, SUB_CATEGORY, BRAND,
                        UNIT_PRICE, COST_PRICE, SUPPLIER_ID, SUPPLIER_NAME,
                        WEIGHT, DIMENSIONS, COLOR, SIZE, MATERIAL, STATUS,
                        PROFIT_MARGIN, PRICE_RANGE, LOAD_DATE, SOURCE_SYSTEM
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                
                # Prepare data for batch insert
                records = []
                for _, row in df.iterrows():
                    records.append((
                        row['PRODUCT_ID'],
                        row['PRODUCT_NAME_CLEAN'],
                        row['PRODUCT_DESC_CLEAN'],
                        row['CATEGORY_CLEAN'],
                        row['SUB_CATEGORY_CLEAN'],
                        row['BRAND_CLEAN'],
                        row['UNIT_PRICE_CLEAN'],
                        row['COST_PRICE_CLEAN'],
                        row['SUPPLIER_ID'],
                        row['SUPPLIER_NAME_CLEAN'],
                        row['WEIGHT_CLEAN'],
                        row['DIMENSIONS'],
                        row['COLOR'],
                        row['SIZE'],
                        row['MATERIAL'],
                        row['STATUS_CLEAN'],
                        row['PROFIT_MARGIN'],
                        row['PRICE_RANGE'],
                        row['LOAD_DATE'],
                        row['SOURCE_SYSTEM']
                    ))
                
                # Execute batch insert
                execute_batch(
                    cursor,
                    insert_sql,
                    records,
                    page_size=self.staging_config['batch_size']
                )
                
                conn.commit()
                logger.info(f"Successfully loaded {len(records)} product records to staging")
                return len(records)
                
        except Exception as e:
            logger.error(f"Error loading product staging: {e}")
            raise


def load_staging_tables(config: Dict[str, Any]) -> Dict[str, int]:
    """Main function to load all staging tables"""
    loader = StagingLoader(config)
    results = {}
    
    try:
        # Load customer staging
        customer_file = config['source']['file_path'] + config['source']['customer_file']
        results['customer_records'] = loader.load_customer_staging(customer_file)
        
        # Load product staging
        product_file = config['source']['file_path'] + config['source']['product_file']
        results['product_records'] = loader.load_product_staging(product_file)
        
        logger.info(f"Staging load completed: {results}")
        return results
        
    except Exception as e:
        logger.error(f"Error in staging load process: {e}")
        raise