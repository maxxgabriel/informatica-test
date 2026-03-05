"""
Transform module for Customer Dimension Load
Implements data cleansing and SCD Type 2 logic
"""
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class CustomerTransformer:
    """Transforms and cleanses customer data with SCD Type 2 logic"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
    def cleanse_text(self, text: Optional[str], case: str = 'upper') -> str:
        """
        Cleanse and standardize text fields
        
        Args:
            text: Input text
            case: 'upper', 'lower', or 'initcap'
            
        Returns:
            Cleansed text
        """
        if not text:
            return ''
            
        # Trim whitespace
        cleaned = text.strip()
        
        # Apply case transformation
        if case == 'upper':
            cleaned = cleaned.upper()
        elif case == 'lower':
            cleaned = cleaned.lower()
        elif case == 'initcap':
            cleaned = cleaned.title()
            
        return cleaned
    
    def cleanse_phone(self, phone: Optional[str]) -> str:
        """
        Cleanse phone number by removing non-numeric characters
        
        Args:
            phone: Input phone number
            
        Returns:
            Cleansed phone number
        """
        if not phone:
            return ''
            
        # Remove all non-numeric characters
        cleaned = re.sub(r'[^0-9]', '', phone)
        return cleaned
    
    def cleanse_customer_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply data cleansing transformations to customer record
        
        Args:
            record: Raw customer record
            
        Returns:
            Cleansed customer record
        """
        cleansed = {}
        
        # Keep original ID
        cleansed['CUSTOMER_ID'] = record.get('CUSTOMER_ID', '')
        
        # Cleanse name fields
        cleansed['FIRST_NAME_CLEAN'] = self.cleanse_text(
            record.get('FIRST_NAME'), 'upper'
        )
        cleansed['LAST_NAME_CLEAN'] = self.cleanse_text(
            record.get('LAST_NAME'), 'upper'
        )
        cleansed['FULL_NAME'] = f"{cleansed['FIRST_NAME_CLEAN']} {cleansed['LAST_NAME_CLEAN']}"
        
        # Cleanse contact information
        cleansed['EMAIL_CLEAN'] = self.cleanse_text(
            record.get('EMAIL'), 'lower'
        )
        cleansed['PHONE_CLEAN'] = self.cleanse_phone(record.get('PHONE'))
        
        # Cleanse address fields
        cleansed['ADDRESS_CLEAN'] = self.cleanse_text(
            record.get('ADDRESS'), 'initcap'
        )
        cleansed['CITY_CLEAN'] = self.cleanse_text(
            record.get('CITY'), 'upper'
        )
        cleansed['STATE_CLEAN'] = self.cleanse_text(
            record.get('STATE'), 'upper'
        )
        cleansed['ZIP_CODE_CLEAN'] = self.cleanse_text(
            record.get('ZIP_CODE'), 'upper'
        ).strip()
        cleansed['COUNTRY_CLEAN'] = self.cleanse_text(
            record.get('COUNTRY'), 'upper'
        )
        
        # Cleanse business fields
        cleansed['CUSTOMER_TYPE_CLEAN'] = self.cleanse_text(
            record.get('CUSTOMER_TYPE'), 'upper'
        )
        
        # Preserve other fields
        cleansed['REGISTRATION_DATE'] = record.get('REGISTRATION_DATE')
        cleansed['SOURCE_SYSTEM'] = record.get('SOURCE_SYSTEM', 'CSV_FILE')
        
        return cleansed
    
    def detect_changes(
        self, 
        new_record: Dict[str, Any], 
        existing_record: Optional[Dict[str, Any]]
    ) -> Tuple[bool, bool]:
        """
        Determine if record is new or changed (SCD Type 2 logic)
        
        Args:
            new_record: Cleansed new record
            existing_record: Existing dimension record (if any)
            
        Returns:
            Tuple of (is_new_record, is_changed)
        """
        # New record if no existing record found
        if not existing_record:
            return (True, False)
        
        # Compare key fields to detect changes
        change_fields = [
            'FIRST_NAME_CLEAN',
            'LAST_NAME_CLEAN',
            'EMAIL_CLEAN',
            'PHONE_CLEAN',
            'ADDRESS_CLEAN'
        ]
        
        is_changed = False
        for field in change_fields:
            new_value = new_record.get(field, '')
            existing_value = existing_record.get(field.replace('_CLEAN', ''), '')
            
            if new_value != existing_value:
                logger.debug(
                    f"Change detected in {field} for customer {new_record.get('CUSTOMER_ID')}: "
                    f"'{existing_value}' -> '{new_value}'"
                )
                is_changed = True
                break
        
        return (False, is_changed)
    
    def prepare_dimension_record(
        self,
        cleansed_record: Dict[str, Any],
        customer_key: int,
        is_new: bool,
        is_changed: bool
    ) -> Dict[str, Any]:
        """
        Prepare final dimension record with SCD Type 2 attributes
        
        Args:
            cleansed_record: Cleansed customer data
            customer_key: Surrogate key value
            is_new: Whether this is a new customer
            is_changed: Whether the customer data changed
            
        Returns:
            Complete dimension record ready for load
        """
        current_time = datetime.now()
        
        dimension_record = {
            'CUSTOMER_KEY': customer_key,
            'CUSTOMER_ID': cleansed_record['CUSTOMER_ID'],
            'FIRST_NAME': cleansed_record['FIRST_NAME_CLEAN'],
            'LAST_NAME': cleansed_record['LAST_NAME_CLEAN'],
            'FULL_NAME': cleansed_record['FULL_NAME'],
            'EMAIL': cleansed_record['EMAIL_CLEAN'],
            'PHONE': cleansed_record['PHONE_CLEAN'],
            'ADDRESS': cleansed_record['ADDRESS_CLEAN'],
            'CITY': cleansed_record['CITY_CLEAN'],
            'STATE': cleansed_record['STATE_CLEAN'],
            'ZIP_CODE': cleansed_record['ZIP_CODE_CLEAN'],
            'COUNTRY': cleansed_record['COUNTRY_CLEAN'],
            'REGISTRATION_DATE': cleansed_record['REGISTRATION_DATE'],
            'CUSTOMER_TYPE': cleansed_record['CUSTOMER_TYPE_CLEAN'],
            'EFFECTIVE_FROM_DATE': current_time,
            'EFFECTIVE_TO_DATE': datetime(9999, 12, 31),
            'IS_CURRENT': 'Y',
            'CREATED_DATE': current_time,
            'SOURCE_SYSTEM': cleansed_record['SOURCE_SYSTEM']
        }
        
        return dimension_record
    
    def transform_batch(
        self, 
        records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Transform a batch of customer records
        
        Args:
            records: List of raw customer records
            
        Returns:
            List of cleansed customer records
        """
        transformed = []
        
        for record in records:
            try:
                cleansed = self.cleanse_customer_record(record)
                transformed.append(cleansed)
            except Exception as e:
                logger.error(
                    f"Error transforming record {record.get('CUSTOMER_ID')}: {e}"
                )
                
        logger.info(f"Transformed {len(transformed)} of {len(records)} records")
        return transformed


def create_transformer(config: Dict[str, Any]) -> CustomerTransformer:
    """Factory function to create transformer instance"""
    return CustomerTransformer(config)