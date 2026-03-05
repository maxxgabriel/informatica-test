"""
Extract module for Customer Dimension ETL
Reads customer data from staging table with incremental load support
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class CustomerExtractor:
    """Extract customer data from staging table"""
    
    def __init__(self, db_engine: Engine, config: Dict):
        """
        Initialize extractor
        
        Args:
            db_engine: SQLAlchemy database engine
            config: Configuration dictionary
        """
        self.engine = db_engine
        self.config = config
        self.staging_table = config.get('staging_table', 'STG_CUSTOMER')
        
    def extract_incremental(self, last_extract_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Extract customer records since last extract date
        
        Args:
            last_extract_date: Date of last extraction. If None, uses config default
            
        Returns:
            DataFrame with customer records
        """
        if last_extract_date is None:
            last_extract_date = self.config.get('default_extract_date', 
                                               datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))
        
        query = text(f"""
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
                SOURCE_SYSTEM,
                LOAD_DATE
            FROM {self.staging_table}
            WHERE LOAD_DATE >= :last_extract_date
            ORDER BY CUSTOMER_ID
        """)
        
        try:
            logger.info(f"Extracting customers from {self.staging_table} since {last_extract_date}")
            
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={'last_extract_date': last_extract_date})
            
            logger.info(f"Extracted {len(df)} customer records")
            
            # Validate extracted data
            self._validate_extraction(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting customer data: {str(e)}")
            raise
    
    def extract_full(self) -> pd.DataFrame:
        """
        Extract all customer records (full refresh)
        
        Returns:
            DataFrame with all customer records
        """
        query = text(f"""
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
                SOURCE_SYSTEM,
                LOAD_DATE
            FROM {self.staging_table}
            ORDER BY CUSTOMER_ID
        """)
        
        try:
            logger.info(f"Extracting all customers from {self.staging_table}")
            
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)
            
            logger.info(f"Extracted {len(df)} customer records (full)")
            
            self._validate_extraction(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting customer data (full): {str(e)}")
            raise
    
    def _validate_extraction(self, df: pd.DataFrame) -> None:
        """
        Validate extracted data
        
        Args:
            df: DataFrame to validate
            
        Raises:
            ValueError: If validation fails
        """
        if df.empty:
            logger.warning("No records extracted")
            return
        
        # Check for required columns
        required_columns = ['CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check for null customer IDs
        null_count = df['CUSTOMER_ID'].isnull().sum()
        if null_count > 0:
            logger.warning(f"Found {null_count} records with null CUSTOMER_ID")
        
        # Check for duplicates
        dup_count = df.duplicated(subset=['CUSTOMER_ID']).sum()
        if dup_count > 0:
            logger.warning(f"Found {dup_count} duplicate CUSTOMER_ID values")
        
        logger.info("Extraction validation completed")
    
    def get_record_count(self, last_extract_date: Optional[datetime] = None) -> int:
        """
        Get count of records to be extracted
        
        Args:
            last_extract_date: Date of last extraction
            
        Returns:
            Number of records to extract
        """
        if last_extract_date is None:
            last_extract_date = self.config.get('default_extract_date',
                                               datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))
        
        query = text(f"""
            SELECT COUNT(*) as cnt
            FROM {self.staging_table}
            WHERE LOAD_DATE >= :last_extract_date
        """)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {'last_extract_date': last_extract_date})
                count = result.scalar()
            
            logger.info(f"Record count for extraction: {count}")
            return count
            
        except Exception as e:
            logger.error(f"Error getting record count: {str(e)}")
            raise


class DimensionLookup:
    """Lookup existing dimension records for SCD processing"""
    
    def __init__(self, db_engine: Engine, config: Dict):
        """
        Initialize lookup
        
        Args:
            db_engine: SQLAlchemy database engine
            config: Configuration dictionary
        """
        self.engine = db_engine
        self.config = config
        self.dimension_table = config.get('dimension_table', 'DIM_CUSTOMER')
    
    def lookup_current_records(self, customer_ids: List[str]) -> pd.DataFrame:
        """
        Lookup current dimension records for given customer IDs
        
        Args:
            customer_ids: List of customer IDs to lookup
            
        Returns:
            DataFrame with current dimension records
        """
        if not customer_ids:
            return pd.DataFrame()
        
        # Create placeholders for IN clause
        placeholders = ','.join([f':id_{i}' for i in range(len(customer_ids))])
        params = {f'id_{i}': cid for i, cid in enumerate(customer_ids)}
        
        query = text(f"""
            SELECT
                CUSTOMER_KEY,
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
                EFFECTIVE_FROM_DATE,
                EFFECTIVE_TO_DATE,
                IS_CURRENT
            FROM {self.dimension_table}
            WHERE CUSTOMER_ID IN ({placeholders})
            AND IS_CURRENT = 'Y'
        """)
        
        try:
            logger.info(f"Looking up {len(customer_ids)} customer records in dimension")
            
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params=params)
            
            logger.info(f"Found {len(df)} existing dimension records")
            
            return df
            
        except Exception as e:
            logger.error(f"Error looking up dimension records: {str(e)}")
            raise
    
    def get_max_customer_key(self) -> int:
        """
        Get maximum customer key value
        
        Returns:
            Maximum customer key or 0 if table is empty
        """
        query = text(f"""
            SELECT COALESCE(MAX(CUSTOMER_KEY), 0) as max_key
            FROM {self.dimension_table}
        """)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query)
                max_key = result.scalar()
            
            logger.info(f"Maximum CUSTOMER_KEY: {max_key}")
            return max_key
            
        except Exception as e:
            logger.error(f"Error getting max customer key: {str(e)}")
            raise