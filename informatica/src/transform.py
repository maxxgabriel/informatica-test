"""
Transform module for Customer Dimension ETL
Implements data cleansing, standardization, and SCD Type 2 logic
"""

import logging
import re
from datetime import datetime, date
from typing import Dict, Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class CustomerTransformer:
    """Transform and cleanse customer data"""
    
    def __init__(self, config: Dict):
        """
        Initialize transformer
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.default_country = config.get('default_country', 'USA')
        self.default_effective_to = config.get('default_effective_to', '9999-12-31')
    
    def cleanse_and_standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply cleansing and standardization transformations
        
        Args:
            df: Input DataFrame with raw customer data
            
        Returns:
            DataFrame with cleansed data
        """
        logger.info("Starting data cleansing and standardization")
        
        df_clean = df.copy()
        
        # Text cleansing - UPPER/LTRIM/RTRIM for names
        df_clean['FIRST_NAME_CLEAN'] = df_clean['FIRST_NAME'].apply(self._clean_text_upper)
        df_clean['LAST_NAME_CLEAN'] = df_clean['LAST_NAME'].apply(self._clean_text_upper)
        
        # Create full name
        df_clean['FULL_NAME'] = df_clean['FIRST_NAME_CLEAN'] + ' ' + df_clean['LAST_NAME_CLEAN']
        
        # Email normalization - lowercase and trim
        df_clean['EMAIL_CLEAN'] = df_clean['EMAIL'].apply(self._normalize_email)
        
        # Phone number standardization - remove special characters
        df_clean['PHONE_CLEAN'] = df_clean['PHONE'].apply(self._standardize_phone)
        
        # Address cleansing - INITCAP and trim
        df_clean['ADDRESS_CLEAN'] = df_clean['ADDRESS'].apply(self._clean_text_initcap)
        
        # City - UPPER and trim
        df_clean['CITY_CLEAN'] = df_clean['CITY'].apply(self._clean_text_upper)
        
        # State - UPPER
        df_clean['STATE_CLEAN'] = df_clean['STATE'].apply(self._clean_state)
        
        # ZIP code - trim
        df_clean['ZIP_CODE_CLEAN'] = df_clean['ZIP_CODE'].apply(self._clean_zip_code)
        
        # Country - UPPER
        df_clean['COUNTRY_CLEAN'] = df_clean['COUNTRY'].apply(
            lambda x: self._clean_text_upper(x) if pd.notna(x) else self.default_country
        )
        
        # Customer type - UPPER
        df_clean['CUSTOMER_TYPE_CLEAN'] = df_clean['CUSTOMER_TYPE'].apply(self._clean_text_upper)
        
        # Add current timestamp
        df_clean['CURRENT_TIMESTAMP'] = datetime.now()
        
        logger.info(f"Cleansing completed for {len(df_clean)} records")
        
        return df_clean
    
    def _clean_text_upper(self, text) -> str:
        """Apply UPPER and LTRIM/RTRIM"""
        if pd.isna(text):
            return ''
        return str(text).strip().upper()
    
    def _clean_text_initcap(self, text) -> str:
        """Apply INITCAP (title case) and LTRIM/RTRIM"""
        if pd.isna(text):
            return ''
        return str(text).strip().title()
    
    def _normalize_email(self, email) -> str:
        """Normalize email - lowercase and trim"""
        if pd.isna(email):
            return ''
        
        email_clean = str(email).strip().lower()
        
        # Basic email validation
        if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email_clean):
            logger.warning(f"Invalid email format: {email_clean}")
        
        return email_clean
    
    def _standardize_phone(self, phone) -> str:
        """
        Standardize phone number - remove (), -, and spaces
        Implements REPLACECHR(0, PHONE, '()-', '')
        """
        if pd.isna(phone):
            return ''
        
        phone_str = str(phone)
        
        # Remove common phone formatting characters
        phone_clean = re.sub(r'[\(\)\-\s]', '', phone_str)
        
        # Keep only digits and plus sign
        phone_clean = re.sub(r'[^\d\+]', '', phone_clean)
        
        return phone_clean
    
    def _clean_state(self, state) -> str:
        """Clean state code - UPPER and validate length"""
        if pd.isna(state):
            return ''
        
        state_clean = str(state).strip().upper()
        
        # Validate 2-character state code
        if len(state_clean) > 2:
            state_clean = state_clean[:2]
        
        return state_clean
    
    def _clean_zip_code(self, zip_code) -> str:
        """Clean ZIP code - trim and standardize format"""
        if pd.isna(zip_code):
            return ''
        
        zip_clean = str(zip_code).strip()
        
        # Remove non-alphanumeric characters except hyphen
        zip_clean = re.sub(r'[^0-9\-]', '', zip_clean)
        
        return zip_clean


class SCDType2Processor:
    """Process SCD Type 2 logic for customer dimension"""
    
    def __init__(self, config: Dict):
        """
        Initialize SCD processor
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.effective_to_max = pd.to_datetime(config.get('default_effective_to', '9999-12-31'))
    
    def process_scd(
        self, 
        source_df: pd.DataFrame, 
        existing_df: pd.DataFrame,
        start_key: int = 1
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Process SCD Type 2 logic
        
        Args:
            source_df: Source data with cleansed fields
            existing_df: Existing dimension records
            start_key: Starting value for new surrogate keys
            
        Returns:
            Tuple of (new_records, changed_records, unchanged_records)
        """
        logger.info("Processing SCD Type 2 logic")
        
        # Merge source with existing on CUSTOMER_ID
        merged = source_df.merge(
            existing_df,
            on='CUSTOMER_ID',
            how='left',
            suffixes=('', '_EXISTING')
        )
        
        # Identify new records (no match in dimension)
        merged['IS_NEW_RECORD'] = merged['CUSTOMER_KEY'].isna()
        
        # Identify changed records
        merged['IS_CHANGED'] = ~merged['IS_NEW_RECORD'] & (
            (merged['FIRST_NAME_CLEAN'] != merged['FIRST_NAME']) |
            (merged['LAST_NAME_CLEAN'] != merged['LAST_NAME']) |
            (merged['EMAIL_CLEAN'] != merged['EMAIL']) |
            (merged['PHONE_CLEAN'] != merged['PHONE']) |
            (merged['ADDRESS_CLEAN'] != merged['ADDRESS'])
        )
        
        # Generate new customer keys
        new_or_changed = merged['IS_NEW_RECORD'] | merged['IS_CHANGED']
        num_new_keys = new_or_changed.sum()
        
        merged['NEW_CUSTOMER_KEY'] = np.nan
        merged.loc[new_or_changed, 'NEW_CUSTOMER_KEY'] = range(start_key, start_key + num_new_keys)
        merged.loc[~new_or_changed, 'NEW_CUSTOMER_KEY'] = merged.loc[~new_or_changed, 'CUSTOMER_KEY']
        
        # Set effective dates
        current_date = datetime.now()
        merged['EFFECTIVE_FROM'] = current_date
        merged['EFFECTIVE_TO'] = self.effective_to_max
        merged['IS_CURRENT_FLAG'] = 'Y'
        
        # Split into groups
        new_records = merged[merged['IS_NEW_RECORD']].copy()
        changed_records = merged[merged['IS_CHANGED']].copy()
        unchanged_records = merged[~merged['IS_NEW_RECORD'] & ~merged['IS_CHANGED']].copy()
        
        logger.info(f"SCD processing complete: {len(new_records)} new, {len(changed_records)} changed, {len(unchanged_records)} unchanged")
        
        return new_records, changed_records, unchanged_records
    
    def prepare_inserts(self, new_records: pd.DataFrame, changed_records: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare records for insertion
        
        Args:
            new_records: New customer records
            changed_records: Changed customer records (new versions)
            
        Returns:
            DataFrame ready for insertion
        """
        inserts = pd.concat([new_records, changed_records], ignore_index=True)
        
        if inserts.empty:
            return pd.DataFrame()
        
        # Select and rename columns for target
        insert_df = pd.DataFrame({
            'CUSTOMER_KEY': inserts['NEW_CUSTOMER_KEY'].astype(int),
            'CUSTOMER_ID': inserts['CUSTOMER_ID'],
            'FIRST_NAME': inserts['FIRST_NAME_CLEAN'],
            'LAST_NAME': inserts['LAST_NAME_CLEAN'],
            'FULL_NAME': inserts['FULL_NAME'],
            'EMAIL': inserts['EMAIL_CLEAN'],
            'PHONE': inserts['PHONE_CLEAN'],
            'ADDRESS': inserts['ADDRESS_CLEAN'],
            'CITY': inserts['CITY_CLEAN'],
            'STATE': inserts['STATE_CLEAN'],
            'ZIP_CODE': inserts['ZIP_CODE_CLEAN'],
            'COUNTRY': inserts['COUNTRY_CLEAN'],
            'REGISTRATION_DATE': inserts['REGISTRATION_DATE'],
            'CUSTOMER_TYPE': inserts['CUSTOMER_TYPE_CLEAN'],
            'EFFECTIVE_FROM_DATE': inserts['EFFECTIVE_FROM'],
            'EFFECTIVE_TO_DATE': inserts['EFFECTIVE_TO'],
            'IS_CURRENT': inserts['IS_CURRENT_FLAG'],
            'CREATED_DATE': datetime.now(),
            'SOURCE_SYSTEM': inserts['SOURCE_SYSTEM']
        })
        
        logger.info(f"Prepared {len(insert_df)} records for insertion")
        
        return insert_df
    
    def prepare_updates(self, changed_records: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare old versions for expiration (update)
        
        Args:
            changed_records: Changed customer records
            
        Returns:
            DataFrame with keys to update
        """
        if changed_records.empty:
            return pd.DataFrame()
        
        yesterday = datetime.now().date() - pd.Timedelta(days=1)
        
        update_df = pd.DataFrame({
            'CUSTOMER_KEY': changed_records['CUSTOMER_KEY'].astype(int),
            'EFFECTIVE_TO_DATE': yesterday,
            'IS_CURRENT': 'N',
            'UPDATED_DATE': datetime.now()
        })
        
        logger.info(f"Prepared {len(update_df)} records for expiration")
        
        return update_df


class DataQualityValidator:
    """Validate data quality after transformation"""
    
    def __init__(self, config: Dict):
        """
        Initialize validator
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.min_name_length = config.get('min_name_length', 1)
        self.max_name_length = config.get('max_name_length', 100)
    
    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate transformed data
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (valid_records, invalid_records)
        """
        logger.info("Validating data quality")
        
        # Create validation flags
        validations = pd.DataFrame({
            'has_customer_id': df['CUSTOMER_ID'].notna(),
            'has_first_name': (df['FIRST_NAME_CLEAN'].notna()) & (df['FIRST_NAME_CLEAN'].str.len() >= self.min_name_length),
            'has_last_name': (df['LAST_NAME_CLEAN'].notna()) & (df['LAST_NAME_CLEAN'].str.len() >= self.min_name_length),
            'valid_email': df['EMAIL_CLEAN'].str.contains('@', na=False) | (df['EMAIL_CLEAN'] == ''),
            'valid_state': (df['STATE_CLEAN'].str.len() <= 2) | (df['STATE_CLEAN'] == '')
        })
        
        # All validations must pass
        is_valid = validations.all(axis=1)
        
        valid_df = df[is_valid].copy()
        invalid_df = df[~is_valid].copy()
        
        # Add validation failure reasons
        if not invalid_df.empty:
            invalid_df['VALIDATION_ERRORS'] = validations[~is_valid].apply(
                lambda row: ', '.join([col for col, val in row.items() if not val]),
                axis=1
            )
        
        logger.info(f"Validation complete: {len(valid_df)} valid, {len(invalid_df)} invalid records")
        
        if len(invalid_df) > 0:
            logger.warning(f"Invalid records found: {invalid_df[['CUSTOMER_ID', 'VALIDATION_ERRORS']].to_dict('records')}")
        
        return valid_df, invalid_df