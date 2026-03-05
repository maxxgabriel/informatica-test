"""
Extract module for Sales Fact ETL
Reads sales transactions from staging table with incremental logic
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class SalesExtractor:
    """Extract sales data from staging table"""
    
    def __init__(self, config: Dict):
        """
        Initialize extractor with database configuration
        
        Args:
            config: Configuration dictionary with database connection details
        """
        self.config = config
        self.engine: Optional[Engine] = None
        
    def connect(self) -> None:
        """Establish database connection"""
        try:
            db_config = self.config['database']
            connection_string = (
                f"{db_config['type']}://{db_config['username']}:{db_config['password']}"
                f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )
            self.engine = create_engine(connection_string, pool_pre_ping=True)
            logger.info("Database connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def extract_sales_transactions(self, last_extract_date: Optional[str] = None) -> pd.DataFrame:
        """
        Extract sales transactions from staging table
        
        Args:
            last_extract_date: Last extract date for incremental load (format: YYYY-MM-DD)
            
        Returns:
            DataFrame containing sales transactions
        """
        if not self.engine:
            self.connect()
        
        extract_config = self.config['extract']
        
        # Build SQL query with incremental logic
        if last_extract_date:
            filter_clause = f"WHERE LOAD_DATE >= TO_DATE('{last_extract_date}', 'YYYY-MM-DD')"
        else:
            # Full load
            filter_clause = ""
        
        query = f"""
        SELECT
            TRANSACTION_ID,
            TRANSACTION_DATE,
            CUSTOMER_ID,
            PRODUCT_ID,
            QUANTITY,
            UNIT_PRICE,
            DISCOUNT_PERCENT,
            TAX_AMOUNT,
            TOTAL_AMOUNT,
            PAYMENT_METHOD,
            STORE_ID,
            REGION,
            SOURCE_SYSTEM
        FROM {extract_config['staging_table']}
        {filter_clause}
        ORDER BY TRANSACTION_DATE
        """
        
        try:
            logger.info(f"Extracting sales transactions from {extract_config['staging_table']}")
            df = pd.read_sql(query, self.engine)
            logger.info(f"Extracted {len(df)} sales transaction records")
            
            # Data validation
            self._validate_extracted_data(df)
            
            return df
        except Exception as e:
            logger.error(f"Failed to extract sales data: {str(e)}")
            raise
    
    def _validate_extracted_data(self, df: pd.DataFrame) -> None:
        """
        Validate extracted data for basic quality checks
        
        Args:
            df: DataFrame to validate
        """
        validation_config = self.config.get('validation', {})
        
        # Check for required columns
        required_columns = [
            'TRANSACTION_ID', 'TRANSACTION_DATE', 'CUSTOMER_ID', 
            'PRODUCT_ID', 'QUANTITY', 'TOTAL_AMOUNT'
        ]
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check for null values in critical fields
        critical_nulls = df[required_columns].isnull().sum()
        if critical_nulls.any():
            logger.warning(f"Found null values in critical fields:\n{critical_nulls[critical_nulls > 0]}")
        
        # Check for negative quantities or amounts
        if (df['QUANTITY'] <= 0).any():
            invalid_count = (df['QUANTITY'] <= 0).sum()
            logger.warning(f"Found {invalid_count} records with invalid quantity (<=0)")
        
        if (df['TOTAL_AMOUNT'] <= 0).any():
            invalid_count = (df['TOTAL_AMOUNT'] <= 0).sum()
            logger.warning(f"Found {invalid_count} records with invalid total amount (<=0)")
        
        logger.info("Data validation completed")
    
    def get_last_load_date(self) -> Optional[str]:
        """
        Get the last successful load date from control table
        
        Returns:
            Last load date as string (YYYY-MM-DD) or None
        """
        if not self.engine:
            self.connect()
        
        control_config = self.config.get('control_table', {})
        if not control_config.get('enabled', False):
            return None
        
        query = f"""
        SELECT MAX(LAST_EXTRACT_DATE) as LAST_DATE
        FROM {control_config['table_name']}
        WHERE TABLE_NAME = 'FACT_SALES'
        AND STATUS = 'SUCCESS'
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                row = result.fetchone()
                if row and row[0]:
                    return row[0].strftime('%Y-%m-%d')
            return None
        except Exception as e:
            logger.warning(f"Could not retrieve last load date: {str(e)}")
            return None
    
    def close(self) -> None:
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


def main():
    """Main execution function for testing"""
    import yaml
    
    # Load configuration
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize extractor
    extractor = SalesExtractor(config)
    
    try:
        # Get last load date
        last_date = extractor.get_last_load_date()
        logger.info(f"Last load date: {last_date}")
        
        # Extract data
        df = extractor.extract_sales_transactions(last_date)
        
        # Display sample
        print(f"\nExtracted {len(df)} records")
        print("\nSample data:")
        print(df.head())
        
    finally:
        extractor.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    main()