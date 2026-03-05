"""
Extract module for Customer Dimension Load
Reads from staging table with incremental logic
"""
import logging
from typing import Dict, List, Any
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class CustomerExtractor:
    """Extracts customer data from staging table"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.staging_conn_config = config['database']['staging']
        self.staging_table = config['sources']['staging_customer_table']
        self.extract_query = config['queries']['extract_staging_customer']
        
    def get_connection(self):
        """Create database connection to staging"""
        try:
            conn = psycopg2.connect(
                host=self.staging_conn_config['url'].split('//')[1].split(':')[0],
                port=int(self.staging_conn_config['url'].split(':')[-1].split('/')[0]),
                database=self.staging_conn_config['url'].split('/')[-1],
                user=self.staging_conn_config['username'],
                password=self.staging_conn_config['password']
            )
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to staging database: {e}")
            raise
    
    def extract_customers(self, last_extract_date: datetime = None) -> List[Dict[str, Any]]:
        """
        Extract customer records from staging table
        
        Args:
            last_extract_date: Date to extract from (defaults to yesterday)
            
        Returns:
            List of customer records as dictionaries
        """
        if last_extract_date is None:
            last_extract_date = datetime.now() - timedelta(days=1)
            
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            logger.info(f"Extracting customers from staging table since {last_extract_date}")
            cursor.execute(self.extract_query, (last_extract_date,))
            
            records = cursor.fetchall()
            logger.info(f"Extracted {len(records)} customer records from staging")
            
            return [dict(record) for record in records]
            
        except Exception as e:
            logger.error(f"Error extracting customers: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """
        Validate customer record has required fields
        
        Args:
            record: Customer record dictionary
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME']
        
        for field in required_fields:
            if not record.get(field):
                logger.warning(f"Record missing required field {field}: {record.get('CUSTOMER_ID')}")
                return False
                
        return True
    
    def extract_and_validate(self, last_extract_date: datetime = None) -> List[Dict[str, Any]]:
        """
        Extract and validate customer records
        
        Args:
            last_extract_date: Date to extract from
            
        Returns:
            List of validated customer records
        """
        records = self.extract_customers(last_extract_date)
        
        valid_records = []
        invalid_count = 0
        
        for record in records:
            if self.validate_record(record):
                valid_records.append(record)
            else:
                invalid_count += 1
                
        logger.info(f"Validation complete: {len(valid_records)} valid, {invalid_count} invalid")
        
        return valid_records


def create_extractor(config: Dict[str, Any]) -> CustomerExtractor:
    """Factory function to create extractor instance"""
    return CustomerExtractor(config)