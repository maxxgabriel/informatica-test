"""
Extract module for Product Dimension ETL
Reads product data from staging table
"""
import logging
from typing import List, Dict, Any
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class ProductExtractor:
    """Extract product data from staging table"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize extractor with configuration
        
        Args:
            config: Database and extraction configuration
        """
        self.config = config
        self.db_config = config['database']
        self.extract_config = config['extract']
        self.connection = None
        
    def connect(self) -> None:
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def disconnect(self) -> None:
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
            
    def extract_products(self, last_extract_date: datetime = None) -> List[Dict[str, Any]]:
        """
        Extract product records from staging table
        
        Args:
            last_extract_date: Extract records loaded after this date (incremental)
            
        Returns:
            List of product records as dictionaries
        """
        if not self.connection:
            self.connect()
            
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                query = self._build_extract_query(last_extract_date)
                logger.info(f"Executing extract query: {query}")
                
                cursor.execute(query, {'last_extract_date': last_extract_date} if last_extract_date else None)
                products = cursor.fetchall()
                
                logger.info(f"Extracted {len(products)} product records")
                return [dict(row) for row in products]
                
        except Exception as e:
            logger.error(f"Error extracting products: {e}")
            raise
            
    def _build_extract_query(self, last_extract_date: datetime = None) -> str:
        """
        Build SQL query for extraction
        
        Args:
            last_extract_date: Optional date filter for incremental loads
            
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
            FROM {staging_table}
        """.format(staging_table=self.extract_config['staging_table'])
        
        if last_extract_date:
            base_query += "\nWHERE LOAD_DATE >= %(last_extract_date)s"
            
        base_query += "\nORDER BY PRODUCT_ID"
        
        return base_query
        
    def get_last_extract_date(self) -> datetime:
        """
        Get last successful extract date from control table
        
        Returns:
            Last extract date or None for full load
        """
        if not self.connection:
            self.connect()
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT MAX(last_extract_date) as last_date
                    FROM etl_control
                    WHERE table_name = %s
                    AND status = 'SUCCESS'
                """, (self.extract_config['staging_table'],))
                
                result = cursor.fetchone()
                last_date = result[0] if result and result[0] else None
                
                logger.info(f"Last extract date: {last_date}")
                return last_date
                
        except Exception as e:
            logger.warning(f"Could not retrieve last extract date: {e}")
            return None
            
    def update_extract_control(self, extract_date: datetime, status: str, records_count: int) -> None:
        """
        Update control table with extract statistics
        
        Args:
            extract_date: Current extract date
            status: SUCCESS or FAILURE
            records_count: Number of records extracted
        """
        if not self.connection:
            self.connect()
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO etl_control 
                    (table_name, last_extract_date, status, records_count, updated_date)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    self.extract_config['staging_table'],
                    extract_date,
                    status,
                    records_count,
                    datetime.now()
                ))
                
                self.connection.commit()
                logger.info(f"Control table updated: {status}, {records_count} records")
                
        except Exception as e:
            logger.error(f"Failed to update control table: {e}")
            self.connection.rollback()


def extract_product_data(config: Dict[str, Any], incremental: bool = True) -> List[Dict[str, Any]]:
    """
    Main function to extract product data
    
    Args:
        config: Configuration dictionary
        incremental: If True, perform incremental extract; if False, full extract
        
    Returns:
        List of product records
    """
    extractor = ProductExtractor(config)
    
    try:
        extractor.connect()
        
        # Get last extract date for incremental load
        last_extract_date = extractor.get_last_extract_date() if incremental else None
        
        # Extract products
        products = extractor.extract_products(last_extract_date)
        
        # Update control table
        extractor.update_extract_control(
            extract_date=datetime.now(),
            status='SUCCESS',
            records_count=len(products)
        )
        
        return products
        
    except Exception as e:
        logger.error(f"Extract failed: {e}")
        extractor.update_extract_control(
            extract_date=datetime.now(),
            status='FAILURE',
            records_count=0
        )
        raise
        
    finally:
        extractor.disconnect()


if __name__ == "__main__":
    # Test extraction
    import yaml
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    products = extract_product_data(config, incremental=True)
    print(f"Extracted {len(products)} products")