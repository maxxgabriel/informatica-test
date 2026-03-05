"""
Extract module for Full Refresh workflow
Handles file ingestion from source to staging tables with truncate-all pattern
"""

import logging
from typing import Dict, List, Optional
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)


class ExtractProcessor:
    """Handles extraction and staging table loads with truncate pattern"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.staging_config = self.config['database']['staging']
        self.source_config = self.config['sources']
        self.perf_config = self.config['performance']
    
    def get_truncate_staging_sql(self) -> List[str]:
        """Generate SQL statements to truncate all staging tables"""
        schema = self.staging_config['schema']
        tables = self.staging_config['tables']
        
        truncate_statements = []
        for table in tables:
            sql = f"TRUNCATE TABLE {schema}.{table}"
            truncate_statements.append(sql)
            logger.info(f"Generated truncate SQL: {sql}")
        
        return truncate_statements
    
    def get_disable_constraints_sql(self) -> List[str]:
        """Generate SQL to disable foreign key constraints"""
        schema = self.staging_config['schema']
        
        # Generic approach for PostgreSQL/Oracle
        sql_statements = [
            f"-- Disable constraints on staging schema",
            f"ALTER TABLE {schema}.STG_CUSTOMER DISABLE CONSTRAINT ALL",
            f"ALTER TABLE {schema}.STG_PRODUCT DISABLE CONSTRAINT ALL",
            f"ALTER TABLE {schema}.STG_SALES DISABLE CONSTRAINT ALL"
        ]
        
        return sql_statements
    
    def get_enable_constraints_sql(self) -> List[str]:
        """Generate SQL to re-enable foreign key constraints"""
        schema = self.staging_config['schema']
        
        sql_statements = [
            f"-- Enable constraints on staging schema",
            f"ALTER TABLE {schema}.STG_CUSTOMER ENABLE CONSTRAINT ALL",
            f"ALTER TABLE {schema}.STG_PRODUCT ENABLE CONSTRAINT ALL",
            f"ALTER TABLE {schema}.STG_SALES ENABLE CONSTRAINT ALL"
        ]
        
        return sql_statements
    
    def get_staging_insert_sql(self, table_name: str) -> str:
        """Generate parameterized INSERT SQL for bulk loading"""
        schema = self.staging_config['schema']
        
        # Table-specific column mappings
        column_mappings = {
            'STG_CUSTOMER': [
                'CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME', 'EMAIL', 'PHONE',
                'ADDRESS', 'CITY', 'STATE', 'ZIP_CODE', 'COUNTRY',
                'REGISTRATION_DATE', 'CUSTOMER_TYPE', 'LOAD_DATE', 'SOURCE_SYSTEM'
            ],
            'STG_PRODUCT': [
                'PRODUCT_ID', 'PRODUCT_NAME', 'PRODUCT_DESCRIPTION', 'CATEGORY',
                'SUB_CATEGORY', 'BRAND', 'UNIT_PRICE', 'COST_PRICE',
                'SUPPLIER_ID', 'SUPPLIER_NAME', 'WEIGHT', 'DIMENSIONS',
                'COLOR', 'SIZE', 'MATERIAL', 'STATUS', 'LOAD_DATE', 'SOURCE_SYSTEM'
            ],
            'STG_SALES': [
                'TRANSACTION_ID', 'TRANSACTION_DATE', 'CUSTOMER_ID', 'PRODUCT_ID',
                'QUANTITY', 'UNIT_PRICE', 'DISCOUNT_PERCENT', 'TAX_AMOUNT',
                'TOTAL_AMOUNT', 'PAYMENT_METHOD', 'STORE_ID', 'REGION',
                'LOAD_DATE', 'SOURCE_SYSTEM'
            ]
        }
        
        columns = column_mappings.get(table_name, [])
        if not columns:
            raise ValueError(f"Unknown table: {table_name}")
        
        placeholders = ', '.join(['?' for _ in columns])
        column_list = ', '.join(columns)
        
        sql = f"""
        INSERT INTO {schema}.{table_name} ({column_list})
        VALUES ({placeholders})
        """
        
        return sql.strip()
    
    def get_file_pattern(self, entity: str) -> str:
        """Get file pattern for entity type"""
        patterns = self.source_config['patterns']
        return patterns.get(entity, '')
    
    def get_source_file_path(self) -> str:
        """Get base source file path"""
        return self.source_config['file_path']
    
    def get_bulk_load_config(self) -> Dict:
        """Get bulk load performance configuration"""
        return {
            'commit_interval': self.perf_config['bulk_load']['commit_interval'],
            'batch_size': self.perf_config['bulk_load']['batch_size'],
            'enabled': self.perf_config['bulk_load']['enabled']
        }
    
    def validate_source_files(self) -> Dict[str, bool]:
        """Validate that all required source files exist"""
        base_path = Path(self.get_source_file_path())
        patterns = self.source_config['patterns']
        
        validation_results = {}
        for entity, pattern in patterns.items():
            if '*' in pattern:
                # Check for at least one matching file
                matching_files = list(base_path.glob(pattern))
                validation_results[entity] = len(matching_files) > 0
                logger.info(f"Found {len(matching_files)} files for {entity}")
            else:
                # Check exact file
                file_path = base_path / pattern
                validation_results[entity] = file_path.exists()
                logger.info(f"File exists for {entity}: {validation_results[entity]}")
        
        return validation_results


class FileProcessor:
    """Handles file reading and CSV parsing for NiFi flow files"""
    
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.file_config = self.config['sources']['file_format']
    
    def get_csv_reader_properties(self) -> Dict:
        """Get CSV reader properties for NiFi"""
        return {
            'CSV Format': 'RFC 4180',
            'Value Separator': self.file_config['delimiter'],
            'Skip Header Line': str(self.file_config['header']),
            'Quote Character': '"',
            'Escape Character': '\\',
            'Trim Fields': 'true',
            'Charset': self.file_config['charset']
        }
    
    def add_metadata_columns(self, record: Dict) -> Dict:
        """Add metadata columns to record"""
        from datetime import datetime
        
        record['LOAD_DATE'] = datetime.now().isoformat()
        record['SOURCE_SYSTEM'] = 'CSV_FILE'
        
        return record


def create_nifi_extract_flow_properties() -> Dict:
    """Generate NiFi processor properties for extract phase"""
    
    processor_configs = {
        'GetFile': {
            'Input Directory': '${source.file.path}',
            'File Filter': '${source.file.pattern}',
            'Keep Source File': 'false',
            'Recurse Subdirectories': 'false',
            'Polling Interval': '10 sec',
            'Batch Size': '10'
        },
        
        'ExecuteSQL_Truncate': {
            'Database Connection Pooling Service': '${staging.db.pool}',
            'SQL select query': '${truncate.sql}',
            'Max Wait Time': '0 seconds'
        },
        
        'ConvertRecord': {
            'Record Reader': 'CSVReader',
            'Record Writer': 'JsonRecordSetWriter',
            'Include Zero Record FlowFiles': 'false'
        },
        
        'PutDatabaseRecord': {
            'Record Reader': 'JsonRecordSetWriter',
            'Database Connection Pooling Service': '${staging.db.pool}',
            'Statement Type': 'INSERT',
            'Table Name': '${staging.table.name}',
            'Translate Field Names': 'true',
            'Maximum Batch Size': '10000',
            'Rollback On Failure': 'true',
            'Support Fragmented Transactions': 'true'
        }
    }
    
    return processor_configs


if __name__ == "__main__":
    # Test configuration loading
    logging.basicConfig(level=logging.INFO)
    
    extractor = ExtractProcessor()
    
    # Test truncate SQL generation
    truncate_sqls = extractor.get_truncate_staging_sql()
    print("Truncate SQL Statements:")
    for sql in truncate_sqls:
        print(f"  {sql}")
    
    # Test insert SQL generation
    print("\nInsert SQL for STG_CUSTOMER:")
    print(extractor.get_staging_insert_sql('STG_CUSTOMER'))
    
    # Test validation
    print("\nFile validation:")
    results = extractor.validate_source_files()
    for entity, valid in results.items():
        print(f"  {entity}: {'✓' if valid else '✗'}")