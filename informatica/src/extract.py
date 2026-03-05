"""
CSV File Extraction Module for Customer Staging Load
Reads customer CSV files and extracts data for staging
"""

import csv
import logging
from pathlib import Path
from typing import Iterator, Dict, Any, Optional
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


class CustomerCSVExtractor:
    """Extracts customer data from CSV files"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize CSV extractor
        
        Args:
            config: Configuration dictionary containing file paths and settings
        """
        self.source_path = Path(config['source']['file_path'])
        self.file_pattern = config['source']['file_pattern']
        self.delimiter = config['source'].get('delimiter', ',')
        self.encoding = config['source'].get('encoding', 'utf-8')
        self.skip_header = config['source'].get('skip_header', True)
        
    def extract_records(self) -> Iterator[Dict[str, Any]]:
        """
        Extract records from CSV file
        
        Yields:
            Dictionary containing customer record data
            
        Raises:
            FileNotFoundError: If source file doesn't exist
            ValueError: If file is empty or invalid format
        """
        file_path = self._get_source_file()
        
        if not file_path.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")
            
        logger.info(f"Extracting records from: {file_path}")
        record_count = 0
        
        try:
            with open(file_path, 'r', encoding=self.encoding) as csvfile:
                reader = csv.DictReader(csvfile, delimiter=self.delimiter)
                
                for row in reader:
                    record_count += 1
                    
                    # Basic validation
                    if not row.get('CUSTOMER_ID'):
                        logger.warning(f"Skipping row {record_count}: Missing CUSTOMER_ID")
                        continue
                        
                    yield self._prepare_record(row)
                    
            logger.info(f"Successfully extracted {record_count} records")
            
        except csv.Error as e:
            logger.error(f"CSV parsing error at line {record_count}: {e}")
            raise ValueError(f"Invalid CSV format: {e}")
        except Exception as e:
            logger.error(f"Extraction error: {e}")
            raise
            
    def _get_source_file(self) -> Path:
        """
        Get the source file path based on pattern
        
        Returns:
            Path to source file
        """
        if self.file_pattern:
            # Find files matching pattern
            matching_files = list(self.source_path.parent.glob(self.file_pattern))
            if not matching_files:
                raise FileNotFoundError(f"No files matching pattern: {self.file_pattern}")
            # Return most recent file
            return max(matching_files, key=lambda p: p.stat().st_mtime)
        return self.source_path
        
    def _prepare_record(self, row: Dict[str, str]) -> Dict[str, Any]:
        """
        Prepare and validate record from CSV row
        
        Args:
            row: Raw CSV row dictionary
            
        Returns:
            Prepared record dictionary
        """
        return {
            'CUSTOMER_ID': row.get('CUSTOMER_ID', '').strip(),
            'FIRST_NAME': row.get('FIRST_NAME', '').strip(),
            'LAST_NAME': row.get('LAST_NAME', '').strip(),
            'EMAIL': row.get('EMAIL', '').strip(),
            'PHONE': row.get('PHONE', '').strip(),
            'ADDRESS': row.get('ADDRESS', '').strip(),
            'CITY': row.get('CITY', '').strip(),
            'STATE': row.get('STATE', '').strip(),
            'ZIP_CODE': row.get('ZIP_CODE', '').strip(),
            'COUNTRY': row.get('COUNTRY', '').strip(),
            'REGISTRATION_DATE': row.get('REGISTRATION_DATE', '').strip(),
            'CUSTOMER_TYPE': row.get('CUSTOMER_TYPE', '').strip()
        }


class NiFiCSVExtractor:
    """NiFi implementation of CSV extraction using processors"""
    
    def __init__(self, canvas, config: Dict[str, Any]):
        """
        Initialize NiFi CSV extractor
        
        Args:
            canvas: NiFi process group canvas
            config: Configuration dictionary
        """
        self.canvas = canvas
        self.config = config
        
    def create_extraction_flow(self) -> Dict[str, Any]:
        """
        Create NiFi processors for CSV extraction
        
        Returns:
            Dictionary containing created processor IDs
        """
        logger.info("Creating CSV extraction flow in NiFi")
        
        # ListFile processor to monitor directory
        list_file = self._create_list_file_processor()
        
        # FetchFile processor to read file content
        fetch_file = self._create_fetch_file_processor()
        
        # SplitRecord processor to split CSV into records
        split_record = self._create_split_record_processor()
        
        # Connect processors
        self._connect_processors(list_file, fetch_file)
        self._connect_processors(fetch_file, split_record)
        
        return {
            'list_file': list_file.id,
            'fetch_file': fetch_file.id,
            'split_record': split_record.id
        }
        
    def _create_list_file_processor(self):
        """Create ListFile processor for monitoring source directory"""
        processor = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ListFile'),
            location=(100, 100),
            name='List Customer CSV Files',
            config=ProcessorConfigDTO(
                properties={
                    'Input Directory': self.config['source']['file_path'],
                    'File Filter': self.config['source'].get('file_pattern', 'customer_master.csv'),
                    'Recurse Subdirectories': 'false',
                    'Minimum File Age': '0 sec',
                    'Maximum File Age': '30 days',
                    'Minimum File Size': '0 B'
                },
                scheduling_period='60 sec',
                auto_terminated_relationships=['success']
            )
        )
        logger.info(f"Created ListFile processor: {processor.id}")
        return processor
        
    def _create_fetch_file_processor(self):
        """Create FetchFile processor to read file content"""
        processor = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.FetchFile'),
            location=(100, 250),
            name='Fetch Customer CSV',
            config=ProcessorConfigDTO(
                properties={
                    'File to Fetch': '${absolute.path}/${filename}',
                    'Completion Strategy': 'None',
                    'Move Conflict Strategy': 'Rename',
                    'Log level when file not found': 'ERROR'
                },
                auto_terminated_relationships=['not.found', 'permission.denied']
            )
        )
        logger.info(f"Created FetchFile processor: {processor.id}")
        return processor
        
    def _create_split_record_processor(self):
        """Create SplitRecord processor to parse CSV"""
        processor = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.SplitRecord'),
            location=(100, 400),
            name='Split CSV Records',
            config=ProcessorConfigDTO(
                properties={
                    'Record Reader': 'CSVReader',
                    'Record Writer': 'JsonRecordSetWriter',
                    'Records Per Split': '1',
                    'record-reader': self._create_csv_reader_service(),
                    'record-writer': self._create_json_writer_service()
                },
                auto_terminated_relationships=['failure', 'original']
            )
        )
        logger.info(f"Created SplitRecord processor: {processor.id}")
        return processor
        
    def _create_csv_reader_service(self) -> str:
        """Create CSV reader controller service"""
        # This would create a CSV reader service in NiFi
        # Simplified for example
        return 'csv-reader-service-id'
        
    def _create_json_writer_service(self) -> str:
        """Create JSON writer controller service"""
        # This would create a JSON writer service in NiFi
        # Simplified for example
        return 'json-writer-service-id'
        
    def _connect_processors(self, source, destination):
        """Connect two processors"""
        nipyapi.canvas.create_connection(
            source=source,
            target=destination,
            relationships=['success']
        )
        logger.info(f"Connected {source.component.name} to {destination.component.name}")


def extract_customer_data(config: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """
    Main extraction function for customer data
    
    Args:
        config: Configuration dictionary
        
    Yields:
        Customer records
    """
    extractor = CustomerCSVExtractor(config)
    return extractor.extract_records()