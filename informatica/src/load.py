"""
Data Loading Module for Customer Staging Table
Loads transformed customer data into staging database
"""

import logging
from typing import Dict, Any, Iterator, List
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


class CustomerStagingLoader:
    """Loads customer data into staging table"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize loader
        
        Args:
            config: Configuration dictionary containing database connection info
        """
        self.db_config = config['target']['database']
        self.table_name = config['target']['table_name']
        self.batch_size = config['target'].get('batch_size', 1000)
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            self.cursor = self.connection.cursor()
            logger.info("Database connection established")
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
            
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")
        
    def load_records(self, records: Iterator[Dict[str, Any]]) -> int:
        """
        Load records into staging table
        
        Args:
            records: Iterator of transformed customer records
            
        Returns:
            Number of records loaded
            
        Raises:
            psycopg2.Error: If database operation fails
        """
        if not self.connection:
            self.connect()
            
        insert_sql = self._build_insert_statement()
        batch = []
        total_loaded = 0
        
        try:
            for record in records:
                batch.append(self._prepare_record_for_insert(record))
                
                if len(batch) >= self.batch_size:
                    loaded = self._insert_batch(insert_sql, batch)
                    total_loaded += loaded
                    logger.info(f"Loaded batch of {loaded} records. Total: {total_loaded}")
                    batch = []
                    
            # Load remaining records
            if batch:
                loaded = self._insert_batch(insert_sql, batch)
                total_loaded += loaded
                logger.info(f"Loaded final batch of {loaded} records")
                
            self.connection.commit()
            logger.info(f"Successfully loaded {total_loaded} records to {self.table_name}")
            
            return total_loaded
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error loading records: {e}")
            raise
            
    def _build_insert_statement(self) -> str:
        """
        Build parameterized INSERT statement
        
        Returns:
            SQL INSERT statement
        """
        columns = [
            'RECORD_ID', 'CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME', 
            'EMAIL', 'PHONE', 'ADDRESS', 'CITY', 'STATE', 'ZIP_CODE',
            'COUNTRY', 'REGISTRATION_DATE', 'CUSTOMER_TYPE',
            'LOAD_DATE', 'SOURCE_SYSTEM'
        ]
        
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        return f"""
            INSERT INTO {self.table_name} ({columns_str})
            VALUES ({placeholders})
        """
        
    def _prepare_record_for_insert(self, record: Dict[str, Any]) -> tuple:
        """
        Prepare record tuple for database insert
        
        Args:
            record: Transformed customer record
            
        Returns:
            Tuple of values for insert
        """
        return (
            record.get('RECORD_ID'),
            record.get('CUSTOMER_ID'),
            record.get('FIRST_NAME'),
            record.get('LAST_NAME'),
            record.get('EMAIL'),
            record.get('PHONE'),
            record.get('ADDRESS'),
            record.get('CITY'),
            record.get('STATE'),
            record.get('ZIP_CODE'),
            record.get('COUNTRY'),
            record.get('REGISTRATION_DATE'),
            record.get('CUSTOMER_TYPE'),
            record.get('LOAD_DATE'),
            record.get('SOURCE_SYSTEM')
        )
        
    def _insert_batch(self, sql: str, batch: List[tuple]) -> int:
        """
        Insert batch of records
        
        Args:
            sql: INSERT statement
            batch: List of record tuples
            
        Returns:
            Number of records inserted
        """
        execute_batch(self.cursor, sql, batch, page_size=self.batch_size)
        return len(batch)
        
    def truncate_table(self):
        """Truncate staging table"""
        try:
            self.cursor.execute(f"TRUNCATE TABLE {self.table_name}")
            self.connection.commit()
            logger.info(f"Truncated table: {self.table_name}")
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Error truncating table: {e}")
            raise


class NiFiLoader:
    """NiFi implementation of data loading"""
    
    def __init__(self, canvas, config: Dict[str, Any]):
        """
        Initialize NiFi loader
        
        Args:
            canvas: NiFi process group canvas
            config: Configuration dictionary
        """
        self.canvas = canvas
        self.config = config
        
    def create_loading_flow(self) -> Dict[str, Any]:
        """
        Create NiFi processors for database loading
        
        Returns:
            Dictionary containing created processor IDs
        """
        logger.info("Creating database loading flow in NiFi")
        
        # ConvertRecord to prepare for database
        convert_record = self._create_convert_record_processor()
        
        # PutDatabaseRecord to insert into staging table
        put_database = self._create_put_database_processor()
        
        # LogAttribute for success tracking
        log_success = self._create_log_processor('success')
        
        # LogAttribute for failure tracking
        log_failure = self._create_log_processor('failure')
        
        # Connect processors
        self._connect_processors(convert_record, put_database)
        nipyapi.canvas.create_connection(
            source=put_database,
            target=log_success,
            relationships=['success']
        )
        nipyapi.canvas.create_connection(
            source=put_database,
            target=log_failure,
            relationships=['failure', 'retry']
        )
        
        return {
            'convert_record': convert_record.id,
            'put_database': put_database.id,
            'log_success': log_success.id,
            'log_failure': log_failure.id
        }
        
    def _create_convert_record_processor(self):
        """Create ConvertRecord processor"""
        processor = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ConvertRecord'),
            location=(100, 850),
            name='Prepare for Database',
            config=ProcessorConfigDTO(
                properties={
                    'Record Reader': 'JsonRecordReader',
                    'Record Writer': 'JsonRecordSetWriter',
                    'Include Zero Record FlowFiles': 'false'
                },
                auto_terminated_relationships=['failure']
            )
        )
        logger.info(f"Created ConvertRecord processor: {processor.id}")
        return processor
        
    def _create_put_database_processor(self):
        """Create PutDatabaseRecord processor"""
        processor = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutDatabaseRecord'),
            location=(100, 1000),
            name='Load to Staging Table',
            config=ProcessorConfigDTO(
                properties={
                    'Record Reader': 'JsonRecordReader',
                    'Statement Type': 'INSERT',
                    'Database Connection Pooling Service': self._create_dbcp_service(),
                    'Table Name': self.config['target']['table_name'],
                    'Maximum Batch Size': str(self.config['target'].get('batch_size', 1000)),
                    'Transaction Timeout': '300 sec',
                    'Rollback On Failure': 'true'
                }
            )
        )
        logger.info(f"Created PutDatabaseRecord processor: {processor.id}")
        return processor
        
    def _create_log_processor(self, log_type: str):
        """Create LogAttribute processor for tracking"""
        processor = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.LogAttribute'),
            location=(300 if log_type == 'success' else 500, 1150),
            name=f'Log {log_type.title()}',
            config=ProcessorConfigDTO(
                properties={
                    'Log Level': 'info' if log_type == 'success' else 'error',
                    'Log Payload': 'false',
                    'Attributes to Log': 'record.count,fragment.count',
                    'Attributes to Log Regex': '.*'
                },
                auto_terminated_relationships=['success']
            )
        )
        logger.info(f"Created LogAttribute processor for {log_type}: {processor.id}")
        return processor
        
    def _create_dbcp_service(self) -> str:
        """Create database connection pool service"""
        # This would create a DBCP service in NiFi
        # Simplified for example
        return 'dbcp-service-id'
        
    def _connect_processors(self, source, destination):
        """Connect two processors"""
        nipyapi.canvas.create_connection(
            source=source,
            target=destination,
            relationships=['success']
        )
        logger.info(f"Connected {source.component.name} to {destination.component.name}")


def load_customer_data(
    records: Iterator[Dict[str, Any]], 
    config: Dict[str, Any]
) -> int:
    """
    Main loading function
    
    Args:
        records: Iterator of transformed customer records
        config: Configuration dictionary
        
    Returns:
        Number of records loaded
    """
    loader = CustomerStagingLoader(config)
    
    try:
        loader.connect()
        record_count = loader.load_records(records)
        return record_count
    finally:
        loader.disconnect()