"""
Load module for Customer Dimension
Implements SCD Type 2 upsert logic using CUSTOMER_KEY
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch

logger = logging.getLogger(__name__)


class CustomerDimensionLoader:
    """Loads customer dimension with SCD Type 2 logic"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.target_conn_config = config['database']['target']
        self.target_table = config['targets']['customer_dimension_table']
        self.sequence_name = config['sequences']['customer_key_sequence']
        
        # SQL queries from config
        self.lookup_query = config['queries']['lookup_existing_customer']
        self.expire_query = config['queries']['expire_old_record']
        self.insert_query = config['queries']['insert_new_record']
        self.sequence_query = config['queries']['get_next_customer_key']
        
        self.batch_size = config['processing']['batch_size']
        self.commit_interval = config['processing']['commit_interval']
        
    def get_connection(self):
        """Create database connection to target warehouse"""
        try:
            conn = psycopg2.connect(
                host=self.target_conn_config['url'].split('//')[1].split(':')[0],
                port=int(self.target_conn_config['url'].split(':')[-1].split('/')[0]),
                database=self.target_conn_config['url'].split('/')[-1],
                user=self.target_conn_config['username'],
                password=self.target_conn_config['password']
            )
            conn.autocommit = False
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to target database: {e}")
            raise
    
    def get_next_customer_key(self, cursor) -> int:
        """
        Get next value from customer key sequence
        
        Args:
            cursor: Database cursor
            
        Returns:
            Next sequence value
        """
        cursor.execute(self.sequence_query)
        result = cursor.fetchone()
        return result[0]
    
    def lookup_existing_customer(
        self, 
        cursor, 
        customer_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Lookup existing active customer record
        
        Args:
            cursor: Database cursor
            customer_id: Natural key to lookup
            
        Returns:
            Existing record dict or None
        """
        try:
            cursor.execute(self.lookup_query, (customer_id,))
            result = cursor.fetchone()
            
            if result:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, result))
            return None
            
        except Exception as e:
            logger.error(f"Error looking up customer {customer_id}: {e}")
            raise
    
    def expire_existing_record(self, cursor, customer_key: int) -> None:
        """
        Expire existing customer record (SCD Type 2)
        
        Args:
            cursor: Database cursor
            customer_key: Surrogate key of record to expire
        """
        try:
            cursor.execute(self.expire_query, (customer_key,))
            logger.debug(f"Expired customer key {customer_key}")
        except Exception as e:
            logger.error(f"Error expiring customer key {customer_key}: {e}")
            raise
    
    def insert_dimension_record(
        self, 
        cursor, 
        record: Dict[str, Any]
    ) -> None:
        """
        Insert new dimension record
        
        Args:
            cursor: Database cursor
            record: Complete dimension record
        """
        try:
            values = (
                record['CUSTOMER_KEY'],
                record['CUSTOMER_ID'],
                record['FIRST_NAME'],
                record['LAST_NAME'],
                record['FULL_NAME'],
                record['EMAIL'],
                record['PHONE'],
                record['ADDRESS'],
                record['CITY'],
                record['STATE'],
                record['ZIP_CODE'],
                record['COUNTRY'],
                record['REGISTRATION_DATE'],
                record['CUSTOMER_TYPE'],
                record['EFFECTIVE_FROM_DATE'],
                record['EFFECTIVE_TO_DATE'],
                record['IS_CURRENT'],
                record['CREATED_DATE'],
                record['SOURCE_SYSTEM']
            )
            
            cursor.execute(self.insert_query, values)
            logger.debug(f"Inserted customer key {record['CUSTOMER_KEY']}")
            
        except Exception as e:
            logger.error(
                f"Error inserting customer {record.get('CUSTOMER_ID')}: {e}"
            )
            raise
    
    def process_scd_type2(
        self,
        cursor,
        cleansed_record: Dict[str, Any],
        existing_record: Optional[Dict[str, Any]],
        is_new: bool,
        is_changed: bool
    ) -> Dict[str, Any]:
        """
        Process SCD Type 2 logic for a single record
        
        Args:
            cursor: Database cursor
            cleansed_record: Cleansed customer data
            existing_record: Existing dimension record if any
            is_new: Whether this is a new customer
            is_changed: Whether customer data changed
            
        Returns:
            Dimension record that was processed
        """
        if is_new:
            # New customer - get new key and insert
            customer_key = self.get_next_customer_key(cursor)
            logger.debug(f"New customer {cleansed_record['CUSTOMER_ID']} - key {customer_key}")
            
        elif is_changed:
            # Changed customer - expire old, insert new version
            old_customer_key = existing_record['CUSTOMER_KEY']
            self.expire_existing_record(cursor, old_customer_key)
            
            customer_key = self.get_next_customer_key(cursor)
            logger.debug(
                f"Changed customer {cleansed_record['CUSTOMER_ID']} - "
                f"expired {old_customer_key}, new key {customer_key}"
            )
            
        else:
            # No change - skip insert
            logger.debug(f"Unchanged customer {cleansed_record['CUSTOMER_ID']} - skipped")
            return None
        
        # Prepare and insert dimension record
        from src.transform import CustomerTransformer
        transformer = CustomerTransformer(self.config)
        
        dimension_record = transformer.prepare_dimension_record(
            cleansed_record, customer_key, is_new, is_changed
        )
        
        self.insert_dimension_record(cursor, dimension_record)
        
        return dimension_record
    
    def load_dimension_batch(
        self,
        cleansed_records: List[Dict[str, Any]],
        scd_analysis: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Load batch of customer dimension records with SCD Type 2
        
        Args:
            cleansed_records: List of cleansed customer records
            scd_analysis: List of dicts with existing_record, is_new, is_changed
            
        Returns:
            Dictionary with load statistics
        """
        conn = None
        stats = {
            'new_records': 0,
            'changed_records': 0,
            'unchanged_records': 0,
            'errors': 0
        }
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            processed_count = 0
            
            for cleansed, analysis in zip(cleansed_records, scd_analysis):
                try:
                    result = self.process_scd_type2(
                        cursor,
                        cleansed,
                        analysis['existing_record'],
                        analysis['is_new'],
                        analysis['is_changed']
                    )
                    
                    if analysis['is_new']:
                        stats['new_records'] += 1
                    elif analysis['is_changed']:
                        stats['changed_records'] += 1
                    else:
                        stats['unchanged_records'] += 1
                    
                    processed_count += 1
                    
                    # Commit at intervals
                    if processed_count % self.commit_interval == 0:
                        conn.commit()
                        logger.info(f"Committed {processed_count} records")
                        
                except Exception as e:
                    logger.error(
                        f"Error processing customer {cleansed.get('CUSTOMER_ID')}: {e}"
                    )
                    stats['errors'] += 1
                    conn.rollback()
            
            # Final commit
            conn.commit()
            logger.info(
                f"Load complete: {stats['new_records']} new, "
                f"{stats['changed_records']} changed, "
                f"{stats['unchanged_records']} unchanged, "
                f"{stats['errors']} errors"
            )
            
        except Exception as e:
            logger.error(f"Error loading dimension batch: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
                
        return stats


def create_loader(config: Dict[str, Any]) -> CustomerDimensionLoader:
    """Factory function to create loader instance"""
    return CustomerDimensionLoader(config)