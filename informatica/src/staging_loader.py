"""
NiFi staging loader module for loading CSV files to staging tables.
Handles customer, product, and sales staging loads with metadata enrichment.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


class StagingLoader:
    """Load CSV files to staging tables with metadata."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize staging loader.
        
        Args:
            config: Configuration dictionary with database and file settings
        """
        self.config = config
        self.source_system = config.get('source_system', 'CSV_FILE')
        
    def load_customer_staging(self, file_path: str, connection) -> Dict[str, Any]:
        """
        Load customer data from CSV to staging table.
        
        Args:
            file_path: Path to customer CSV file
            connection: Database connection object
            
        Returns:
            Dict with load statistics
        """
        logger.info(f"Loading customer staging from {file_path}")
        
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Add metadata columns
            df['LOAD_DATE'] = datetime.now()
            df['SOURCE_SYSTEM'] = self.source_system
            df['RECORD_ID'] = range(1, len(df) + 1)
            
            # Validate required columns
            required_cols = [
                'CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME', 'EMAIL',
                'PHONE', 'ADDRESS', 'CITY', 'STATE', 'ZIP_CODE',
                'COUNTRY', 'REGISTRATION_DATE', 'CUSTOMER_TYPE'
            ]
            
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Load to staging table
            df.to_sql(
                'STG_CUSTOMER',
                connection,
                if_exists='append',
                index=False,
                chunksize=10000
            )
            
            stats = {
                'table': 'STG_CUSTOMER',
                'rows_loaded': len(df),
                'load_time': datetime.now(),
                'status': 'SUCCESS'
            }
            
            logger.info(f"Successfully loaded {len(df)} customer records to staging")
            return stats
            
        except Exception as e:
            logger.error(f"Error loading customer staging: {str(e)}")
            raise
            
    def load_product_staging(self, file_path: str, connection) -> Dict[str, Any]:
        """
        Load product data from CSV to staging table.
        
        Args:
            file_path: Path to product CSV file
            connection: Database connection object
            
        Returns:
            Dict with load statistics
        """
        logger.info(f"Loading product staging from {file_path}")
        
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Add metadata columns
            df['LOAD_DATE'] = datetime.now()
            df['SOURCE_SYSTEM'] = self.source_system
            df['RECORD_ID'] = range(1, len(df) + 1)
            
            # Validate required columns
            required_cols = [
                'PRODUCT_ID', 'PRODUCT_NAME', 'PRODUCT_DESCRIPTION',
                'CATEGORY', 'SUB_CATEGORY', 'BRAND', 'UNIT_PRICE',
                'COST_PRICE', 'SUPPLIER_ID', 'SUPPLIER_NAME',
                'WEIGHT', 'DIMENSIONS', 'COLOR', 'SIZE', 'MATERIAL', 'STATUS'
            ]
            
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Validate numeric columns
            df['UNIT_PRICE'] = pd.to_numeric(df['UNIT_PRICE'], errors='coerce')
            df['COST_PRICE'] = pd.to_numeric(df['COST_PRICE'], errors='coerce')
            df['WEIGHT'] = pd.to_numeric(df['WEIGHT'], errors='coerce')
            
            # Load to staging table
            df.to_sql(
                'STG_PRODUCT',
                connection,
                if_exists='append',
                index=False,
                chunksize=10000
            )
            
            stats = {
                'table': 'STG_PRODUCT',
                'rows_loaded': len(df),
                'load_time': datetime.now(),
                'status': 'SUCCESS'
            }
            
            logger.info(f"Successfully loaded {len(df)} product records to staging")
            return stats
            
        except Exception as e:
            logger.error(f"Error loading product staging: {str(e)}")
            raise
            
    def load_sales_staging(self, file_path: str, connection) -> Dict[str, Any]:
        """
        Load sales transaction data from CSV to staging table.
        
        Args:
            file_path: Path to sales CSV file
            connection: Database connection object
            
        Returns:
            Dict with load statistics
        """
        logger.info(f"Loading sales staging from {file_path}")
        
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Add metadata columns
            df['LOAD_DATE'] = datetime.now()
            df['SOURCE_SYSTEM'] = self.source_system
            df['RECORD_ID'] = range(1, len(df) + 1)
            
            # Validate required columns
            required_cols = [
                'TRANSACTION_ID', 'TRANSACTION_DATE', 'CUSTOMER_ID',
                'PRODUCT_ID', 'QUANTITY', 'UNIT_PRICE', 'DISCOUNT_PERCENT',
                'TAX_AMOUNT', 'TOTAL_AMOUNT', 'PAYMENT_METHOD',
                'STORE_ID', 'REGION'
            ]
            
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Validate numeric columns
            df['QUANTITY'] = pd.to_numeric(df['QUANTITY'], errors='coerce')
            df['UNIT_PRICE'] = pd.to_numeric(df['UNIT_PRICE'], errors='coerce')
            df['DISCOUNT_PERCENT'] = pd.to_numeric(df['DISCOUNT_PERCENT'], errors='coerce')
            df['TAX_AMOUNT'] = pd.to_numeric(df['TAX_AMOUNT'], errors='coerce')
            df['TOTAL_AMOUNT'] = pd.to_numeric(df['TOTAL_AMOUNT'], errors='coerce')
            
            # Filter invalid records
            df = df[(df['QUANTITY'] > 0) & (df['TOTAL_AMOUNT'] > 0)]
            
            # Parse transaction date
            df['TRANSACTION_DATE'] = pd.to_datetime(df['TRANSACTION_DATE'])
            
            # Load to staging table
            df.to_sql(
                'STG_SALES',
                connection,
                if_exists='append',
                index=False,
                chunksize=10000
            )
            
            stats = {
                'table': 'STG_SALES',
                'rows_loaded': len(df),
                'load_time': datetime.now(),
                'status': 'SUCCESS'
            }
            
            logger.info(f"Successfully loaded {len(df)} sales records to staging")
            return stats
            
        except Exception as e:
            logger.error(f"Error loading sales staging: {str(e)}")
            raise


class DimensionLoader:
    """Load dimension tables with SCD logic."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize dimension loader.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        
    def load_customer_dimension(self, connection, last_extract_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Load customer dimension with SCD Type 2 logic.
        
        Args:
            connection: Database connection object
            last_extract_date: Optional last extract date for incremental loads
            
        Returns:
            Dict with load statistics
        """
        logger.info("Loading customer dimension with SCD Type 2")
        
        try:
            cursor = connection.cursor()
            
            # Build query with optional date filter
            query = """
                SELECT
                    CUSTOMER_ID,
                    FIRST_NAME,
                    LAST_NAME,
                    EMAIL,
                    PHONE,
                    ADDRESS,
                    CITY,
                    STATE,
                    ZIP_CODE,
                    COUNTRY,
                    REGISTRATION_DATE,
                    CUSTOMER_TYPE,
                    SOURCE_SYSTEM
                FROM STG_CUSTOMER
            """
            
            if last_extract_date:
                query += f" WHERE LOAD_DATE >= '{last_extract_date}'"
            
            # Read staging data
            df = pd.read_sql(query, connection)
            
            # Cleanse data
            df['FIRST_NAME_CLEAN'] = df['FIRST_NAME'].str.strip().str.upper()
            df['LAST_NAME_CLEAN'] = df['LAST_NAME'].str.strip().str.upper()
            df['FULL_NAME'] = df['FIRST_NAME_CLEAN'] + ' ' + df['LAST_NAME_CLEAN']
            df['EMAIL_CLEAN'] = df['EMAIL'].str.strip().str.lower()
            df['PHONE_CLEAN'] = df['PHONE'].str.replace(r'[()-]', '', regex=True)
            df['ADDRESS_CLEAN'] = df['ADDRESS'].str.strip().str.title()
            df['CITY_CLEAN'] = df['CITY'].str.strip().str.upper()
            df['STATE_CLEAN'] = df['STATE'].str.strip().str.upper()
            df['ZIP_CODE_CLEAN'] = df['ZIP_CODE'].str.strip()
            df['COUNTRY_CLEAN'] = df['COUNTRY'].str.strip().str.upper()
            df['CUSTOMER_TYPE_CLEAN'] = df['CUSTOMER_TYPE'].str.strip().str.upper()
            
            new_records = 0
            updated_records = 0
            unchanged_records = 0
            
            # Process each record
            for _, row in df.iterrows():
                # Lookup existing record
                lookup_query = """
                    SELECT CUSTOMER_KEY, FIRST_NAME, LAST_NAME, EMAIL, PHONE, ADDRESS
                    FROM DIM_CUSTOMER
                    WHERE CUSTOMER_ID = %s AND IS_CURRENT = 'Y'
                """
                cursor.execute(lookup_query, (row['CUSTOMER_ID'],))
                existing = cursor.fetchone()
                
                if not existing:
                    # New record - insert
                    insert_query = """
                        INSERT INTO DIM_CUSTOMER (
                            CUSTOMER_ID, FIRST_NAME, LAST_NAME, FULL_NAME,
                            EMAIL, PHONE, ADDRESS, CITY, STATE, ZIP_CODE,
                            COUNTRY, REGISTRATION_DATE, CUSTOMER_TYPE,
                            EFFECTIVE_FROM_DATE, EFFECTIVE_TO_DATE, IS_CURRENT,
                            CREATED_DATE, SOURCE_SYSTEM
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, '9999-12-31', 'Y', %s, %s
                        )
                    """
                    cursor.execute(insert_query, (
                        row['CUSTOMER_ID'],
                        row['FIRST_NAME_CLEAN'],
                        row['LAST_NAME_CLEAN'],
                        row['FULL_NAME'],
                        row['EMAIL_CLEAN'],
                        row['PHONE_CLEAN'],
                        row['ADDRESS_CLEAN'],
                        row['CITY_CLEAN'],
                        row['STATE_CLEAN'],
                        row['ZIP_CODE_CLEAN'],
                        row['COUNTRY_CLEAN'],
                        row['REGISTRATION_DATE'],
                        row['CUSTOMER_TYPE_CLEAN'],
                        datetime.now(),
                        datetime.now(),
                        row['SOURCE_SYSTEM']
                    ))
                    new_records += 1
                else:
                    # Check if changed
                    is_changed = (
                        existing[1] != row['FIRST_NAME_CLEAN'] or
                        existing[2] != row['LAST_NAME_CLEAN'] or
                        existing[3] != row['EMAIL_CLEAN'] or
                        existing[4] != row['PHONE_CLEAN'] or
                        existing[5] != row['ADDRESS_CLEAN']
                    )
                    
                    if is_changed:
                        # Expire old record
                        update_query = """
                            UPDATE DIM_CUSTOMER
                            SET EFFECTIVE_TO_DATE = %s,
                                IS_CURRENT = 'N',
                                UPDATED_DATE = %s
                            WHERE CUSTOMER_KEY = %s
                        """
                        cursor.execute(update_query, (
                            datetime.now(),
                            datetime.now(),
                            existing[0]
                        ))
                        
                        # Insert new version
                        insert_query = """
                            INSERT INTO DIM_CUSTOMER (
                                CUSTOMER_ID, FIRST_NAME, LAST_NAME, FULL_NAME,
                                EMAIL, PHONE, ADDRESS, CITY, STATE, ZIP_CODE,
                                COUNTRY, REGISTRATION_DATE, CUSTOMER_TYPE,
                                EFFECTIVE_FROM_DATE, EFFECTIVE_TO_DATE, IS_CURRENT,
                                CREATED_DATE, SOURCE_SYSTEM
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, '9999-12-31', 'Y', %s, %s
                            )
                        """
                        cursor.execute(insert_query, (
                            row['CUSTOMER_ID'],
                            row['FIRST_NAME_CLEAN'],
                            row['LAST_NAME_CLEAN'],
                            row['FULL_NAME'],
                            row['EMAIL_CLEAN'],
                            row['PHONE_CLEAN'],
                            row['ADDRESS_CLEAN'],
                            row['CITY_CLEAN'],
                            row['STATE_CLEAN'],
                            row['ZIP_CODE_CLEAN'],
                            row['COUNTRY_CLEAN'],
                            row['REGISTRATION_DATE'],
                            row['CUSTOMER_TYPE_CLEAN'],
                            datetime.now(),
                            datetime.now(),
                            row['SOURCE_SYSTEM']
                        ))
                        updated_records += 1
                    else:
                        unchanged_records += 1
            
            connection.commit()
            
            stats = {
                'table': 'DIM_CUSTOMER',
                'new_records': new_records,
                'updated_records': updated_records,
                'unchanged_records': unchanged_records,
                'total_processed': len(df),
                'load_time': datetime.now(),
                'status': 'SUCCESS'
            }
            
            logger.info(f"Customer dimension load complete: {stats}")
            return stats
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Error loading customer dimension: {str(e)}")
            raise
            
    def load_product_dimension(self, connection, last_extract_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Load product dimension with SCD Type 1 logic.
        
        Args:
            connection: Database connection object
            last_extract_date: Optional last extract date for incremental loads
            
        Returns:
            Dict with load statistics
        """
        logger.info("Loading product dimension with SCD Type 1")
        
        try:
            cursor = connection.cursor()
            
            # Build query with optional date filter
            query = """
                SELECT
                    PRODUCT_ID,
                    PRODUCT_NAME,
                    PRODUCT_DESCRIPTION,
                    CATEGORY,
                    SUB_CATEGORY,
                    BRAND,
                    UNIT_PRICE,
                    COST_PRICE,
                    SUPPLIER_ID,
                    SUPPLIER_NAME,
                    WEIGHT,
                    DIMENSIONS,
                    COLOR,
                    SIZE,
                    MATERIAL,
                    STATUS,
                    SOURCE_SYSTEM
                FROM STG_PRODUCT
            """
            
            if last_extract_date:
                query += f" WHERE LOAD_DATE >= '{last_extract_date}'"
            
            # Read staging data
            df = pd.read_sql(query, connection)
            
            # Cleanse data
            df['PRODUCT_NAME_CLEAN'] = df['PRODUCT_NAME'].str.strip().str.title()
            df['PRODUCT_DESC_CLEAN'] = df['PRODUCT_DESCRIPTION'].str.strip()
            df['CATEGORY_CLEAN'] = df['CATEGORY'].str.strip().str.upper()
            df['SUB_CATEGORY_CLEAN'] = df['SUB_CATEGORY'].str.strip().str.upper()
            df['BRAND_CLEAN'] = df['BRAND'].str.strip().str.title()
            df['UNIT_PRICE_CLEAN'] = df['UNIT_PRICE'].round(2)
            df['COST_PRICE_CLEAN'] = df['COST_PRICE'].round(2)
            df['SUPPLIER_NAME_CLEAN'] = df['SUPPLIER_NAME'].str.strip().str.title()
            df['STATUS_CLEAN'] = df['STATUS'].str.strip().str.upper()
            
            # Calculate profit margin
            df['PROFIT_MARGIN'] = ((df['UNIT_PRICE_CLEAN'] - df['COST_PRICE_CLEAN']) / 
                                   df['UNIT_PRICE_CLEAN'] * 100).round(2)
            df['PROFIT_MARGIN'] = df['PROFIT_MARGIN'].fillna(0)
            
            # Calculate price range
            df['PRICE_RANGE'] = df['UNIT_PRICE_CLEAN'].apply(
                lambda x: 'LOW' if x < 50 else 
                         'MEDIUM' if x < 200 else 
                         'HIGH' if x < 500 else 'PREMIUM'
            )
            
            # Filter valid products
            df = df[
                df['PRODUCT_ID'].notna() &
                df['PRODUCT_NAME_CLEAN'].notna() &
                df['CATEGORY_CLEAN'].notna() &
                (df['UNIT_PRICE_CLEAN'] >= 0) &
                (df['COST_PRICE_CLEAN'] >= 0)
            ]
            
            new_records = 0
            updated_records = 0
            unchanged_records = 0
            
            # Process each record
            for _, row in df.iterrows():
                # Lookup existing record
                lookup_query = """
                    SELECT PRODUCT_KEY, PRODUCT_NAME, CATEGORY, UNIT_PRICE, STATUS
                    FROM DIM_PRODUCT
                    WHERE PRODUCT_ID = %s
                """
                cursor.execute(lookup_query, (row['PRODUCT_ID'],))
                existing = cursor.fetchone()
                
                if not existing:
                    # New record - insert
                    insert_query = """
                        INSERT INTO DIM_PRODUCT (
                            PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCRIPTION,
                            CATEGORY, SUB_CATEGORY, BRAND, UNIT_PRICE, COST_PRICE,
                            SUPPLIER_ID, SUPPLIER_NAME, WEIGHT, DIMENSIONS,
                            COLOR, SIZE, MATERIAL, STATUS, PROFIT_MARGIN,
                            PRICE_RANGE, CREATED_DATE, UPDATED_DATE, SOURCE_SYSTEM
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    cursor.execute(insert_query, (
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
                        row['WEIGHT'],
                        row['DIMENSIONS'],
                        row['COLOR'],
                        row['SIZE'],
                        row['MATERIAL'],
                        row['STATUS_CLEAN'],
                        row['PROFIT_MARGIN'],
                        row['PRICE_RANGE'],
                        datetime.now(),
                        datetime.now(),
                        row['SOURCE_SYSTEM']
                    ))
                    new_records += 1
                else:
                    # Check if changed (SCD Type 1 - overwrite)
                    is_changed = (
                        existing[1] != row['PRODUCT_NAME_CLEAN'] or
                        existing[2] != row['CATEGORY_CLEAN'] or
                        abs(existing[3] - row['UNIT_PRICE_CLEAN']) > 0.01 or
                        existing[4] != row['STATUS_CLEAN']
                    )
                    
                    if is_changed:
                        # Update existing record
                        update_query = """
                            UPDATE DIM_PRODUCT
                            SET PRODUCT_NAME = %s,
                                PRODUCT_DESCRIPTION = %s,
                                CATEGORY = %s,
                                SUB_CATEGORY = %s,
                                BRAND = %s,
                                UNIT_PRICE = %s,
                                COST_PRICE = %s,
                                SUPPLIER_ID = %s,
                                SUPPLIER_NAME = %s,
                                WEIGHT = %s,
                                DIMENSIONS = %s,
                                COLOR = %s,
                                SIZE = %s,
                                MATERIAL = %s,
                                STATUS = %s,
                                PROFIT_MARGIN = %s,
                                PRICE_RANGE = %s,
                                UPDATED_DATE = %s
                            WHERE PRODUCT_KEY = %s
                        """
                        cursor.execute(update_query, (
                            row['PRODUCT_NAME_CLEAN'],
                            row['PRODUCT_DESC_CLEAN'],
                            row['CATEGORY_CLEAN'],
                            row['SUB_CATEGORY_CLEAN'],
                            row['BRAND_CLEAN'],
                            row['UNIT_PRICE_CLEAN'],
                            row['COST_PRICE_CLEAN'],
                            row['SUPPLIER_ID'],
                            row['SUPPLIER_NAME_CLEAN'],
                            row['WEIGHT'],
                            row['DIMENSIONS'],
                            row['COLOR'],
                            row['SIZE'],
                            row['MATERIAL'],
                            row['STATUS_CLEAN'],
                            row['PROFIT_MARGIN'],
                            row['PRICE_RANGE'],
                            datetime.now(),
                            existing[0]
                        ))
                        updated_records += 1
                    else:
                        unchanged_records += 1
            
            connection.commit()
            
            stats = {
                'table': 'DIM_PRODUCT',
                'new_records': new_records,
                'updated_records': updated_records,
                'unchanged_records': unchanged_records,
                'total_processed': len(df),
                'load_time': datetime.now(),
                'status': 'SUCCESS'
            }
            
            logger.info(f"Product dimension load complete: {stats}")
            return stats
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Error loading product dimension: {str(e)}")
            raise