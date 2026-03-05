"""
Data Transformation Module for Customer Staging Load
Adds metadata fields: LOAD_DATE, SOURCE_SYSTEM, and RECORD_ID
"""

import logging
from typing import Dict, Any, Iterator
from datetime import datetime
from dataclasses import dataclass
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


@dataclass
class TransformationMetadata:
    """Metadata added during transformation"""
    load_date: datetime
    source_system: str
    sequence_start: int = 1


class CustomerDataTransformer:
    """Transforms customer data by adding required metadata fields"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transformer
        
        Args:
            config: Configuration dictionary containing transformation settings
        """
        self.source_system = config['transform']['source_system']
        self.sequence_start = config['transform'].get('sequence_start', 1)
        self.date_format = config['transform'].get('date_format', '%Y-%m-%d %H:%M:%S')
        self._current_sequence = self.sequence_start
        
    def transform_records(self, records: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        """
        Transform records by adding metadata
        
        Args:
            records: Iterator of customer records
            
        Yields:
            Transformed records with metadata
        """
        logger.info("Starting record transformation")
        load_date = datetime.now()
        transform_count = 0
        
        for record in records:
            try:
                transformed = self._add_metadata(record, load_date)
                transform_count += 1
                
                if transform_count % 1000 == 0:
                    logger.info(f"Transformed {transform_count} records")
                    
                yield transformed
                
            except Exception as e:
                logger.error(f"Error transforming record: {record.get('CUSTOMER_ID', 'UNKNOWN')} - {e}")
                # Continue processing other records
                continue
                
        logger.info(f"Transformation complete. Total records: {transform_count}")
        
    def _add_metadata(self, record: Dict[str, Any], load_date: datetime) -> Dict[str, Any]:
        """
        Add metadata fields to record
        
        Args:
            record: Original customer record
            load_date: Current load timestamp
            
        Returns:
            Record with added metadata
        """
        record['LOAD_DATE'] = load_date.strftime(self.date_format)
        record['SOURCE_SYSTEM'] = self.source_system
        record['RECORD_ID'] = self._get_next_sequence()
        
        return record
        
    def _get_next_sequence(self) -> int:
        """
        Get next sequence number for RECORD_ID
        
        Returns:
            Next sequence number
        """
        seq_id = self._current_sequence
        self._current_sequence += 1
        return seq_id
        
    def reset_sequence(self, start_value: int = None):
        """
        Reset sequence counter
        
        Args:
            start_value: New starting value (defaults to config value)
        """
        self._current_sequence = start_value if start_value is not None else self.sequence_start
        logger.info(f"Sequence reset to {self._current_sequence}")


class NiFiTransformer:
    """NiFi implementation of transformation logic"""
    
    def __init__(self, canvas, config: Dict[str, Any]):
        """
        Initialize NiFi transformer
        
        Args:
            canvas: NiFi process group canvas
            config: Configuration dictionary
        """
        self.canvas = canvas
        self.config = config
        
    def create_transformation_flow(self) -> Dict[str, Any]:
        """
        Create NiFi processors for transformation
        
        Returns:
            Dictionary containing created processor IDs
        """
        logger.info("Creating transformation flow in NiFi")
        
        # UpdateRecord processor to add metadata fields
        update_record = self._create_update_record_processor()
        
        # RouteOnAttribute processor for error handling
        route_on_attribute = self._create_route_processor()
        
        # Connect processors
        self._connect_processors(update_record, route_on_attribute)
        
        return {
            'update_record': update_record.id,
            'route_on_attribute': route_on_attribute.id
        }
        
    def _create_update_record_processor(self):
        """Create UpdateRecord processor to add metadata"""
        processor = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.UpdateRecord'),
            location=(100, 550),
            name='Add Metadata Fields',
            config=ProcessorConfigDTO(
                properties={
                    'Record Reader': 'JsonRecordReader',
                    'Record Writer': 'JsonRecordSetWriter',
                    'Replacement Value Strategy': 'Literal Value',
                    '/LOAD_DATE': '${now():format("yyyy-MM-dd HH:mm:ss")}',
                    '/SOURCE_SYSTEM': self.config['transform']['source_system'],
                    '/RECORD_ID': '${UUID()}',  # Using UUID as sequence alternative in NiFi
                },
                auto_terminated_relationships=['failure']
            )
        )
        logger.info(f"Created UpdateRecord processor: {processor.id}")
        return processor
        
    def _create_route_processor(self):
        """Create RouteOnAttribute processor for validation"""
        processor = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnAttribute'),
            location=(100, 700),
            name='Validate Transformed Records',
            config=ProcessorConfigDTO(
                properties={
                    'Routing Strategy': 'Route to Property name',
                    'valid': "${literal('true'):equals(${isEmpty(CUSTOMER_ID):not():and(${isEmpty(LOAD_DATE):not()}):and(${isEmpty(SOURCE_SYSTEM):not()}):and(${isEmpty(RECORD_ID):not()})})}",
                },
                auto_terminated_relationships=['unmatched']
            )
        )
        logger.info(f"Created RouteOnAttribute processor: {processor.id}")
        return processor
        
    def _connect_processors(self, source, destination):
        """Connect two processors"""
        nipyapi.canvas.create_connection(
            source=source,
            target=destination,
            relationships=['success']
        )
        logger.info(f"Connected {source.component.name} to {destination.component.name}")


class DataValidator:
    """Validates transformed records"""
    
    @staticmethod
    def validate_record(record: Dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate transformed record
        
        Args:
            record: Transformed record
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check required original fields
        if not record.get('CUSTOMER_ID'):
            errors.append("Missing CUSTOMER_ID")
        if not record.get('FIRST_NAME'):
            errors.append("Missing FIRST_NAME")
        if not record.get('LAST_NAME'):
            errors.append("Missing LAST_NAME")
            
        # Check required metadata fields
        if not record.get('LOAD_DATE'):
            errors.append("Missing LOAD_DATE")
        if not record.get('SOURCE_SYSTEM'):
            errors.append("Missing SOURCE_SYSTEM")
        if not record.get('RECORD_ID'):
            errors.append("Missing RECORD_ID")
            
        # Validate data types
        try:
            if record.get('RECORD_ID'):
                int(record['RECORD_ID'])
        except (ValueError, TypeError):
            errors.append("RECORD_ID must be numeric")
            
        return len(errors) == 0, errors


def transform_customer_data(
    records: Iterator[Dict[str, Any]], 
    config: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Main transformation function
    
    Args:
        records: Iterator of extracted customer records
        config: Configuration dictionary
        
    Yields:
        Transformed customer records
    """
    transformer = CustomerDataTransformer(config)
    validator = DataValidator()
    
    for record in transformer.transform_records(records):
        is_valid, errors = validator.validate_record(record)
        
        if is_valid:
            yield record
        else:
            logger.warning(f"Invalid record {record.get('CUSTOMER_ID')}: {errors}")