"""
Extract module for product data validation and edge case handling.
Reads product data from staging table with comprehensive error handling.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class ProductDataExtractor:
    """Extract product data from staging with validation."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize extractor with database configuration.
        
        Args:
            config: Configuration dictionary with database connection details
        """
        self.config = config
        self.engine = None
        self.connection_string = self._build_connection_string()
        
    def _build_connection_string(self) -> str:
        """Build database connection string from config."""
        db_config = self.config['database']
        return (
            f"{db_config['type']}://{db_config['username']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
    
    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_size=self.config['database'].get('pool_size', 5),
                max_overflow=self.config['database'].get('max_overflow', 10)
            )
            logger.info("Database connection established successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def disconnect(self) -> None:
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")
    
    def extract_product_data(
        self,
        last_extract_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Extract product data from staging table.
        
        Args:
            last_extract_date: Optional date for incremental extraction
            
        Returns:
            DataFrame containing product data
        """
        try:
            query = self._build_extraction_query(last_extract_date)
            logger.info(f"Executing extraction query: {query}")
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            logger.info(f"Extracted {len(df)} product records")
            return df
            
        except SQLAlchemyError as e:
            logger.error(f"Error extracting product data: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {str(e)}")
            raise
    
    def _build_extraction_query(
        self,
        last_extract_date: Optional[datetime] = None
    ) -> str:
        """
        Build SQL query for product extraction.
        
        Args:
            last_extract_date: Optional date for incremental extraction
            
        Returns:
            SQL query string
        """
        base_query = """
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
                SOURCE_SYSTEM,
                LOAD_DATE
            FROM STG_PRODUCT
        """
        
        if last_extract_date:
            date_str = last_extract_date.strftime('%Y-%m-%d %H:%M:%S')
            base_query += f"\nWHERE LOAD_DATE >= '{date_str}'"
        
        base_query += "\nORDER BY PRODUCT_ID"
        
        return base_query
    
    def get_extraction_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate extraction statistics.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            Dictionary of statistics
        """
        return {
            'total_records': len(df),
            'null_product_ids': df['PRODUCT_ID'].isnull().sum(),
            'null_product_names': df['PRODUCT_NAME'].isnull().sum(),
            'null_unit_prices': df['UNIT_PRICE'].isnull().sum(),
            'null_cost_prices': df['COST_PRICE'].isnull().sum(),
            'zero_prices': len(df[df['UNIT_PRICE'] == 0]),
            'negative_prices': len(df[df['UNIT_PRICE'] < 0]),
            'extraction_timestamp': datetime.now().isoformat()
        }