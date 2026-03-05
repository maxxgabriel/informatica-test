"""
Extract module for m_LOAD_STG_PRODUCT
Reads product CSV file and extracts data for staging
"""
import csv
import logging
from typing import Generator, Dict, Any
from pathlib import Path
from datetime import datetime
import nipyapi

logger = logging.getLogger(__name__)


class ProductExtractor:
    """Extract product data from CSV files"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize extractor with configuration
        
        Args:
            config: Configuration dictionary with source file settings
        """
        self.config = config
        self.source_file_path = Path(config['source']['file_path'])
        self.file_name = config['source']['file_name']
        self.delimiter = config['source'].get('delimiter', ',')
        self.encoding = config['source'].get('encoding', 'utf-8')
        self.skip_header = config['source'].get('skip_header', True)
        
    def extract(self) -> Generator[Dict[str, Any], None, None]:
        """
        Extract product records from CSV file
        
        Yields:
            Dict containing product record data
            
        Raises:
            FileNotFoundError: If source file doesn't exist
            ValueError: If file format is invalid
        """
        file_path = self.source_file_path / self.file_name
        
        if not file_path.exists():
            error_msg = f"Source file not found: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
            
        logger.info(f"Starting extraction from {file_path}")
        record_count = 0
        
        try:
            with open(file_path, 'r', encoding=self.encoding) as csvfile:
                reader = csv.DictReader(csvfile, delimiter=self.delimiter)
                
                if self.skip_header:
                    next(reader, None)
                
                for row in reader:
                    try:
                        record = self._validate_and_extract_record(row)
                        if record:
                            record_count += 1
                            yield record
                    except Exception as e:
                        logger.warning(f"Skipping invalid record at line {record_count + 1}: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise
            
        logger.info(f"Extraction complete. Total records extracted: {record_count}")
    
    def _validate_and_extract_record(self, row: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate and transform CSV row to record dictionary
        
        Args:
            row: CSV row as dictionary
            
        Returns:
            Validated record dictionary
            
        Raises:
            ValueError: If required fields are missing or invalid
        """
        required_fields = [
            'PRODUCT_ID', 'PRODUCT_NAME', 'CATEGORY', 'UNIT_PRICE', 'STATUS'
        ]
        
        # Check required fields
        for field in required_fields:
            if not row.get(field) or row[field].strip() == '':
                raise ValueError(f"Missing required field: {field}")
        
        # Build record with type conversion
        record = {
            'PRODUCT_ID': row['PRODUCT_ID'].strip(),
            'PRODUCT_NAME': row['PRODUCT_NAME'].strip(),
            'PRODUCT_DESCRIPTION': row.get('PRODUCT_DESCRIPTION', '').strip(),
            'CATEGORY': row['CATEGORY'].strip(),
            'SUB_CATEGORY': row.get('SUB_CATEGORY', '').strip(),
            'BRAND': row.get('BRAND', '').strip(),
            'UNIT_PRICE': self._parse_decimal(row['UNIT_PRICE']),
            'COST_PRICE': self._parse_decimal(row.get('COST_PRICE', '0')),
            'SUPPLIER_ID': row.get('SUPPLIER_ID', '').strip(),
            'SUPPLIER_NAME': row.get('SUPPLIER_NAME', '').strip(),
            'WEIGHT': self._parse_decimal(row.get('WEIGHT', '0')),
            'DIMENSIONS': row.get('DIMENSIONS', '').strip(),
            'COLOR': row.get('COLOR', '').strip(),
            'SIZE': row.get('SIZE', '').strip(),
            'MATERIAL': row.get('MATERIAL', '').strip(),
            'STATUS': row['STATUS'].strip()
        }
        
        # Validate business rules
        if record['UNIT_PRICE'] < 0:
            raise ValueError(f"Invalid UNIT_PRICE: {record['UNIT_PRICE']}")
            
        if record['COST_PRICE'] < 0:
            raise ValueError(f"Invalid COST_PRICE: {record['COST_PRICE']}")
        
        return record
    
    @staticmethod
    def _parse_decimal(value: str) -> float:
        """
        Parse string to decimal
        
        Args:
            value: String value to parse
            
        Returns:
            Parsed float value
        """
        try:
            return float(value.strip()) if value.strip() else 0.0
        except ValueError:
            raise ValueError(f"Cannot parse decimal value: {value}")


class NiFiProductExtractor:
    """NiFi-based extractor using GetFile and related processors"""
    
    def __init__(self, config: Dict[str, Any], canvas: Any):
        """
        Initialize NiFi extractor
        
        Args:
            config: Configuration dictionary
            canvas: NiFi canvas object (process group)
        """
        self.config = config
        self.canvas = canvas
        self.processor_config = config['nifi']['processors']['extract']
        
    def create_extract_flow(self) -> Dict[str, Any]:
        """
        Create NiFi flow for product extraction
        
        Returns:
            Dictionary with created processor IDs
        """
        logger.info("Creating NiFi extract flow")
        
        # Create GetFile processor
        get_file = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.GetFile'),
            location=(100, 100),
            name='GetProductFile',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Input Directory': self.config['source']['file_path'],
                    'File Filter': self.config['source']['file_name'],
                    'Keep Source File': 'false',
                    'Polling Interval': '10 sec',
                    'Batch Size': '10'
                },
                auto_terminated_relationships=['not.found']
            )
        )
        
        # Create RouteOnAttribute for error handling
        route_on_attr = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnAttribute'),
            location=(300, 100),
            name='RouteValidFiles',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Routing Strategy': 'Route to Property name',
                    'valid': "${filename:matches('.*\\.csv')}"
                },
                auto_terminated_relationships=['unmatched']
            )
        )
        
        # Create SplitText for CSV processing
        split_text = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.SplitText'),
            location=(500, 100),
            name='SplitCSVRecords',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Line Split Count': '1',
                    'Header Line Count': '1',
                    'Remove Trailing Newlines': 'true'
                },
                auto_terminated_relationships=['failure']
            )
        )
        
        # Create ExtractText for CSV parsing
        extract_text = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExtractText'),
            location=(700, 100),
            name='ParseCSVFields',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Character Set': 'UTF-8',
                    'Maximum Buffer Size': '1 MB',
                    'Enable Canonical Equivalence': 'false',
                    'Enable Case-insensitive Matching': 'true',
                    # CSV field extraction patterns
                    'product.id': '^([^,]+),.*',
                    'product.name': '^[^,]+,([^,]+),.*',
                    'product.category': '^[^,]+,[^,]+,[^,]+,([^,]+),.*'
                },
                auto_terminated_relationships=['unmatched']
            )
        )
        
        # Connect processors
        nipyapi.canvas.create_connection(get_file, route_on_attr, ['success'])
        nipyapi.canvas.create_connection(route_on_attr, split_text, ['valid'])
        nipyapi.canvas.create_connection(split_text, extract_text, ['splits'])
        
        logger.info("Extract flow created successfully")
        
        return {
            'get_file': get_file.id,
            'route_on_attr': route_on_attr.id,
            'split_text': split_text.id,
            'extract_text': extract_text.id
        }


def main():
    """Main extraction function for testing"""
    import yaml
    
    logging.basicConfig(level=logging.INFO)
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Run extraction
    extractor = ProductExtractor(config)
    
    for record in extractor.extract():
        print(record)


if __name__ == '__main__':
    main()