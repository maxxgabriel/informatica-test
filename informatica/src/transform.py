"""
Data Transformation Module for Daily Sales Load
Handles data cleansing, SCD logic, and business calculations
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Dict, Tuple, Optional
import yaml


class DataTransformer:
    """Transform and cleanse data for staging and dimension tables"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize transformer with configuration
        
        Args:
            config_path: Path to configuration file
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """Configure logging"""
        log_path = self.config['logging']['log_path']
        
        logging.basicConfig(
            level=self.config['logging']['level'],
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def cleanse_customer_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleanse and standardize customer data
        Mimics EXP_CLEANSE_CUSTOMER transformation
        
        Args:
            df: Raw customer DataFrame
            
        Returns:
            Cleansed customer DataFrame
        """
        self.logger.info("Starting customer data cleansing")
        
        df_clean = df.copy()
        
        try:
            # Clean name fields - UPPER and LTRIM/RTRIM
            df_clean['FIRST_NAME_CLEAN'] = df_clean['FIRST_NAME'].str.strip().str.upper()
            df_clean['LAST_NAME_CLEAN'] = df_clean['LAST_NAME'].str.strip().str.upper()
            df_clean['FULL_NAME'] = df_clean['FIRST_NAME_CLEAN'] + ' ' + df_clean['LAST_NAME_CLEAN']
            
            # Clean email - LOWER
            df_clean['EMAIL_CLEAN'] = df_clean['EMAIL'].str.strip().str.lower()
            
            # Clean phone - remove special characters
            df_clean['PHONE_CLEAN'] = df_clean['PHONE'].str.replace(r'[()-\s]', '', regex=True)
            
            # Clean address fields
            df_clean['ADDRESS_CLEAN'] = df_clean['ADDRESS'].str.strip().str.title()
            df_clean['CITY_CLEAN'] = df_clean['CITY'].str.strip().str.upper()
            df_clean['STATE_CLEAN'] = df_clean['STATE'].str.strip().str.upper()
            df_clean['ZIP_CODE_CLEAN'] = df_clean['ZIP_CODE'].str.strip()
            df_clean['COUNTRY_CLEAN'] = df_clean['COUNTRY'].str.strip().str.upper()
            df_clean['CUSTOMER_TYPE_CLEAN'] = df_clean['CUSTOMER_TYPE'].str.strip().str.upper()
            
            # Add current timestamp
            df_clean['CURRENT_TIMESTAMP'] = datetime.now()
            
            self.logger.info(f"Cleansed {len(df_clean)} customer records")
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error cleansing customer data: {str(e)}")
            raise
    
    def cleanse_product_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleanse and standardize product data
        Mimics EXP_CLEANSE_PRODUCT transformation
        
        Args:
            df: Raw product DataFrame
            
        Returns:
            Cleansed product DataFrame
        """
        self.logger.info("Starting product data cleansing")
        
        df_clean = df.copy()
        
        try:
            # Clean text fields
            df_clean['PRODUCT_NAME_CLEAN'] = df_clean['PRODUCT_NAME'].str.strip().str.title()
            df_clean['PRODUCT_DESC_CLEAN'] = df_clean['PRODUCT_DESCRIPTION'].str.strip()
            df_clean['CATEGORY_CLEAN'] = df_clean['CATEGORY'].str.strip().str.upper()
            df_clean['SUB_CATEGORY_CLEAN'] = df_clean['SUB_CATEGORY'].str.strip().str.upper()
            df_clean['BRAND_CLEAN'] = df_clean['BRAND'].str.strip().str.title()
            df_clean['SUPPLIER_NAME_CLEAN'] = df_clean['SUPPLIER_NAME'].str.strip().str.title()
            df_clean['STATUS_CLEAN'] = df_clean['STATUS'].str.strip().str.upper()
            
            # Clean numeric fields - round to 2 decimals
            df_clean['UNIT_PRICE_CLEAN'] = df_clean['UNIT_PRICE'].round(2)
            df_clean['COST_PRICE_CLEAN'] = df_clean['COST_PRICE'].round(2)
            
            # Calculate profit margin
            df_clean['PROFIT_MARGIN'] = np.where(
                df_clean['UNIT_PRICE_CLEAN'] > 0,
                ((df_clean['UNIT_PRICE_CLEAN'] - df_clean['COST_PRICE_CLEAN']) / 
                 df_clean['UNIT_PRICE_CLEAN'] * 100).round(2),
                0
            )
            
            # Calculate price range
            df_clean['PRICE_RANGE'] = pd.cut(
                df_clean['UNIT_PRICE_CLEAN'],
                bins=[0, 50, 200, 500, float('inf')],
                labels=['LOW', 'MEDIUM', 'HIGH', 'PREMIUM']
            )
            
            # Add current timestamp
            df_clean['CURRENT_TIMESTAMP'] = datetime.now()
            
            self.logger.info(f"Cleansed {len(df_clean)} product records")
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error cleansing product data: {str(e)}")
            raise
    
    def apply_scd_type2_logic(
        self, 
        source_df: pd.DataFrame, 
        existing_df: pd.DataFrame,
        key_column: str,
        compare_columns: list
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Apply SCD Type 2 logic to dimension data
        Mimics LKP_DIM_CUSTOMER and EXP_SCD_LOGIC transformations
        
        Args:
            source_df: Source data from staging
            existing_df: Existing dimension data
            key_column: Business key column name
            compare_columns: Columns to compare for changes
            
        Returns:
            Tuple of (new_records, changed_records, unchanged_records)
        """
        self.logger.info("Applying SCD Type 2 logic")
        
        try:
            # Filter existing records where IS_CURRENT = 'Y'
            current_records = existing_df[existing_df['IS_CURRENT'] == 'Y'].copy()
            
            # Merge to identify new vs existing records
            merged = source_df.merge(
                current_records,
                on=key_column,
                how='left',
                suffixes=('', '_EXISTING'),
                indicator=True
            )
            
            # Identify new records
            new_records = merged[merged['_merge'] == 'left_only'].copy()
            new_records['IS_NEW_RECORD'] = 1
            new_records['IS_CHANGED'] = 0
            
            # Identify potentially changed records
            existing_matches = merged[merged['_merge'] == 'both'].copy()
            
            # Check for changes in compare columns
            changed_mask = pd.Series(False, index=existing_matches.index)
            for col in compare_columns:
                if col in existing_matches.columns and f"{col}_EXISTING" in existing_matches.columns:
                    changed_mask |= (existing_matches[col] != existing_matches[f"{col}_EXISTING"])
            
            changed_records = existing_matches[changed_mask].copy()
            changed_records['IS_NEW_RECORD'] = 0
            changed_records['IS_CHANGED'] = 1
            
            unchanged_records = existing_matches[~changed_mask].copy()
            unchanged_records['IS_NEW_RECORD'] = 0
            unchanged_records['IS_CHANGED'] = 0
            
            # Add SCD metadata
            current_date = datetime.now()
            end_date = datetime(9999, 12, 31)
            
            for df in [new_records, changed_records]:
                df['EFFECTIVE_FROM_DATE'] = current_date
                df['EFFECTIVE_TO_DATE'] = end_date
                df['IS_CURRENT'] = 'Y'
                df['CREATED_DATE'] = current_date
                df['UPDATED_DATE'] = current_date
            
            self.logger.info(
                f"SCD Type 2 - New: {len(new_records)}, "
                f"Changed: {len(changed_records)}, "
                f"Unchanged: {len(unchanged_records)}"
            )
            
            return new_records, changed_records, unchanged_records
            
        except Exception as e:
            self.logger.error(f"Error applying SCD Type 2 logic: {str(e)}")
            raise
    
    def apply_scd_type1_logic(
        self,
        source_df: pd.DataFrame,
        existing_df: pd.DataFrame,
        key_column: str,
        compare_columns: list
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Apply SCD Type 1 logic to dimension data
        Mimics LKP_DIM_PRODUCT and EXP_SCD_TYPE1 transformations
        
        Args:
            source_df: Source data from staging
            existing_df: Existing dimension data
            key_column: Business key column name
            compare_columns: Columns to compare for changes
            
        Returns:
            Tuple of (new_records, updated_records, unchanged_records)
        """
        self.logger.info("Applying SCD Type 1 logic")
        
        try:
            # Merge to identify new vs existing records
            merged = source_df.merge(
                existing_df,
                on=key_column,
                how='left',
                suffixes=('', '_EXISTING'),
                indicator=True
            )
            
            # Identify new records
            new_records = merged[merged['_merge'] == 'left_only'].copy()
            new_records['IS_NEW_RECORD'] = 1
            new_records['IS_CHANGED'] = 0
            new_records['CREATED_DATE'] = datetime.now()
            
            # Identify existing records
            existing_matches = merged[merged['_merge'] == 'both'].copy()
            
            # Check for changes
            changed_mask = pd.Series(False, index=existing_matches.index)
            for col in compare_columns:
                if col in existing_matches.columns and f"{col}_EXISTING" in existing_matches.columns:
                    changed_mask |= (existing_matches[col] != existing_matches[f"{col}_EXISTING"])
            
            updated_records = existing_matches[changed_mask].copy()
            updated_records['IS_NEW_RECORD'] = 0
            updated_records['IS_CHANGED'] = 1
            updated_records['UPDATED_DATE'] = datetime.now()
            
            unchanged_records = existing_matches[~changed_mask].copy()
            unchanged_records['IS_NEW_RECORD'] = 0
            unchanged_records['IS_CHANGED'] = 0
            
            self.logger.info(
                f"SCD Type 1 - New: {len(new_records)}, "
                f"Updated: {len(updated_records)}, "
                f"Unchanged: {len(unchanged_records)}"
            )
            
            return new_records, updated_records, unchanged_records
            
        except Exception as e:
            self.logger.error(f"Error applying SCD Type 1 logic: {str(e)}")
            raise
    
    def calculate_sales_measures(
        self,
        sales_df: pd.DataFrame,
        product_lookup: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calculate business metrics for sales fact table
        Mimics EXP_CALCULATE_MEASURES transformation
        
        Args:
            sales_df: Sales transaction data
            product_lookup: Product dimension for cost lookup
            
        Returns:
            Sales DataFrame with calculated measures
        """
        self.logger.info("Calculating sales measures")
        
        df = sales_df.copy()
        
        try:
            # Join with product dimension to get cost price
            df = df.merge(
                product_lookup[['PRODUCT_ID', 'COST_PRICE']],
                on='PRODUCT_ID',
                how='left'
            )
            
            # Calculate date key (YYYYMMDD format)
            df['DATE_KEY'] = pd.to_datetime(df['TRANSACTION_DATE']).dt.strftime('%Y%m%d').astype(int)
            
            # Calculate discount amount
            df['DISCOUNT_AMOUNT'] = (
                df['UNIT_PRICE'] * df['QUANTITY'] * df['DISCOUNT_PERCENT'] / 100
            ).round(2)
            
            # Calculate cost amount
            df['COST_AMOUNT'] = (df['COST_PRICE'] * df['QUANTITY']).round(2)
            
            # Calculate profit amount
            df['PROFIT_AMOUNT'] = (
                df['TOTAL_AMOUNT'] - df['TAX_AMOUNT'] - df['COST_AMOUNT']
            ).round(2)
            
            # Calculate profit margin percentage
            df['PROFIT_MARGIN_PERCENT'] = np.where(
                (df['TOTAL_AMOUNT'] - df['TAX_AMOUNT']) > 0,
                (df['PROFIT_AMOUNT'] / (df['TOTAL_AMOUNT'] - df['TAX_AMOUNT']) * 100).round(2),
                0
            )
            
            # Add load timestamp
            df['LOAD_TIMESTAMP'] = datetime.now()
            
            self.logger.info(f"Calculated measures for {len(df)} sales records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error calculating sales measures: {str(e)}")
            raise
    
    def filter_valid_sales_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter valid sales transactions
        Mimics FIL_VALID_RECORDS transformation
        
        Args:
            df: Sales DataFrame
            
        Returns:
            Filtered DataFrame
        """
        self.logger.info("Filtering valid sales records")
        
        initial_count = len(df)
        
        # Apply filters
        valid_df = df[
            df['CUSTOMER_ID'].notna() &
            df['PRODUCT_ID'].notna() &
            (df['QUANTITY'] > 0) &
            (df['TOTAL_AMOUNT'] > 0)
        ].copy()
        
        filtered_count = initial_count - len(valid_df)
        self.logger.info(f"Filtered out {filtered_count} invalid records, kept {len(valid_df)}")
        
        return valid_df
    
    def deduplicate_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate sales transactions
        Mimics AGG_DEDUPLICATE transformation
        
        Args:
            df: Sales DataFrame
            
        Returns:
            Deduplicated DataFrame
        """
        self.logger.info("Deduplicating sales records")
        
        initial_count = len(df)
        
        # Keep first occurrence of each TRANSACTION_ID
        dedup_df = df.drop_duplicates(subset=['TRANSACTION_ID'], keep='first')
        
        duplicate_count = initial_count - len(dedup_df)
        if duplicate_count > 0:
            self.logger.warning(f"Removed {duplicate_count} duplicate transactions")
        
        return dedup_df


def main():
    """Main execution function for testing"""
    transformer = DataTransformer()
    
    # Test customer cleansing
    test_customer = pd.DataFrame({
        'CUSTOMER_ID': ['C001'],
        'FIRST_NAME': ['  john  '],
        'LAST_NAME': ['  doe  '],
        'EMAIL': ['  JOHN.DOE@EMAIL.COM  '],
        'PHONE': ['(555) 123-4567'],
        'ADDRESS': ['123 main street'],
        'CITY': ['  new york  '],
        'STATE': ['ny'],
        'ZIP_CODE': ['10001'],
        'COUNTRY': ['usa'],
        'CUSTOMER_TYPE': ['retail']
    })
    
    cleansed = transformer.cleanse_customer_data(test_customer)
    print("Cleansed Customer Data:")
    print(cleansed[['FIRST_NAME_CLEAN', 'EMAIL_CLEAN', 'PHONE_CLEAN']].head())


if __name__ == "__main__":
    main()