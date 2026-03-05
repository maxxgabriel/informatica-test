"""
Transform module for Full Refresh workflow
Implements data cleansing, SCD logic, and business calculations
"""

import pandas as pd
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, date

from src.database_utils import DatabaseManager

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms staging data with cleansing and business logic"""
    
    def __init__(self, config: Dict[str, Any], db_manager: DatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.customer_key_seq = config['sequence_generators']['customer_key']['start_value']
        self.product_key_seq = config['sequence_generators']['product_key']['start_value']
        self.sales_key_seq = config['sequence_generators']['sales_key']['start_value']
    
    def cleanse_customer_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleanse and standardize customer data"""
        logger.info("Cleansing customer data")
        
        df_clean = df.copy()
        
        # String cleansing
        df_clean['FIRST_NAME'] = df_clean['FIRST_NAME'].str.strip().str.upper()
        df_clean['LAST_NAME'] = df_clean['LAST_NAME'].str.strip().str.upper()
        df_clean['FULL_NAME'] = df_clean['FIRST_NAME'] + ' ' + df_clean['LAST_NAME']
        df_clean['EMAIL'] = df_clean['EMAIL'].str.strip().str.lower()
        
        # Phone number cleaning
        df_clean['PHONE'] = df_clean['PHONE'].str.replace(r'[()-\s]', '', regex=True)
        
        # Address cleaning
        df_clean['ADDRESS'] = df_clean['ADDRESS'].str.strip().str.title()
        df_clean['CITY'] = df_clean['CITY'].str.strip().str.upper()
        df_clean['STATE'] = df_clean['STATE'].str.upper()
        df_clean['ZIP_CODE'] = df_clean['ZIP_CODE'].str.strip()
        df_clean['COUNTRY'] = df_clean['COUNTRY'].str.upper()
        df_clean['CUSTOMER_TYPE'] = df_clean['CUSTOMER_TYPE'].str.upper()
        
        logger.info(f"Cleansed {len(df_clean)} customer records")
        return df_clean
    
    def cleanse_product_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleanse and standardize product data"""
        logger.info("Cleansing product data")
        
        df_clean = df.copy()
        
        # String cleansing
        df_clean['PRODUCT_NAME'] = df_clean['PRODUCT_NAME'].str.strip().str.title()
        df_clean['PRODUCT_DESCRIPTION'] = df_clean['PRODUCT_DESCRIPTION'].str.strip()
        df_clean['CATEGORY'] = df_clean['CATEGORY'].str.strip().str.upper()
        df_clean['SUB_CATEGORY'] = df_clean['SUB_CATEGORY'].str.strip().str.upper()
        df_clean['BRAND'] = df_clean['BRAND'].str.strip().str.title()
        df_clean['SUPPLIER_NAME'] = df_clean['SUPPLIER_NAME'].str.strip().str.title()
        df_clean['STATUS'] = df_clean['STATUS'].str.upper()
        
        # Numeric cleansing
        df_clean['UNIT_PRICE'] = df_clean['UNIT_PRICE'].round(2)
        df_clean['COST_PRICE'] = df_clean['COST_PRICE'].round(2)
        
        # Calculated fields
        df_clean['PROFIT_MARGIN'] = (
            ((df_clean['UNIT_PRICE'] - df_clean['COST_PRICE']) / 
             df_clean['UNIT_PRICE'] * 100)
            .fillna(0)
            .round(2)
        )
        
        df_clean['PRICE_RANGE'] = pd.cut(
            df_clean['UNIT_PRICE'],
            bins=[0, 50, 200, 500, float('inf')],
            labels=['LOW', 'MEDIUM', 'HIGH', 'PREMIUM']
        )
        
        logger.info(f"Cleansed {len(df_clean)} product records")
        return df_clean
    
    def apply_customer_scd_type2(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply SCD Type 2 logic for customer dimension
        
        Args:
            df: Cleansed customer data
            
        Returns:
            DataFrame with SCD Type 2 fields
        """
        logger.info("Applying SCD Type 2 logic for customers")
        
        df_scd = df.copy()
        
        # Generate surrogate keys
        df_scd['CUSTOMER_KEY'] = range(
            self.customer_key_seq,
            self.customer_key_seq + len(df_scd)
        )
        self.customer_key_seq += len(df_scd)
        
        # Add SCD Type 2 fields
        df_scd['EFFECTIVE_FROM_DATE'] = datetime.now()
        df_scd['EFFECTIVE_TO_DATE'] = pd.to_datetime('9999-12-31')
        df_scd['IS_CURRENT'] = 'Y'
        df_scd['CREATED_DATE'] = datetime.now()
        df_scd['UPDATED_DATE'] = None
        
        logger.info(f"Applied SCD Type 2 to {len(df_scd)} customer records")
        return df_scd
    
    def apply_product_scd_type1(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply SCD Type 1 logic for product dimension
        
        Args:
            df: Cleansed product data
            
        Returns:
            DataFrame with surrogate keys
        """
        logger.info("Applying SCD Type 1 logic for products")
        
        df_scd = df.copy()
        
        # Generate surrogate keys
        df_scd['PRODUCT_KEY'] = range(
            self.product_key_seq,
            self.product_key_seq + len(df_scd)
        )
        self.product_key_seq += len(df_scd)
        
        # Add audit fields
        df_scd['CREATED_DATE'] = datetime.now()
        df_scd['UPDATED_DATE'] = None
        
        logger.info(f"Applied SCD Type 1 to {len(df_scd)} product records")
        return df_scd
    
    def filter_valid_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out invalid product records"""
        logger.info("Filtering valid products")
        
        initial_count = len(df)
        
        df_valid = df[
            df['PRODUCT_ID'].notna() &
            df['PRODUCT_NAME'].notna() &
            df['CATEGORY'].notna() &
            (df['UNIT_PRICE'] >= 0) &
            (df['COST_PRICE'] >= 0)
        ].copy()
        
        filtered_count = initial_count - len(df_valid)
        logger.info(f"Filtered {filtered_count} invalid products, {len(df_valid)} remain")
        
        return df_valid
    
    def transform_customer_dimension(self) -> pd.DataFrame:
        """
        Full transformation pipeline for customer dimension
        
        Returns:
            Transformed customer dimension data
        """
        logger.info("Transforming customer dimension")
        
        # Read from staging
        query = f"SELECT * FROM {self.config['staging_tables']['customer']}"
        with self.db_manager.get_staging_connection() as conn:
            df = pd.read_sql(query, conn)
        
        # Apply transformations
        df = self.cleanse_customer_data(df)
        df = self.apply_customer_scd_type2(df)
        
        return df
    
    def transform_product_dimension(self) -> pd.DataFrame:
        """
        Full transformation pipeline for product dimension
        
        Returns:
            Transformed product dimension data
        """
        logger.info("Transforming product dimension")
        
        # Read from staging
        query = f"SELECT * FROM {self.config['staging_tables']['product']}"
        with self.db_manager.get_staging_connection() as conn:
            df = pd.read_sql(query, conn)
        
        # Apply transformations
        df = self.cleanse_product_data(df)
        df = self.filter_valid_products(df)
        df = self.apply_product_scd_type1(df)
        
        return df