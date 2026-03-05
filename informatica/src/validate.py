"""
Data Quality Validation Module for Customer Dimension
Implements validation rules and error handling for customer data
"""

import re
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from dataclasses import dataclass, field

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """Represents a data validation error"""
    record_id: str
    field_name: str
    error_type: str
    error_message: str
    record_data: Dict
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ValidationResult:
    """Represents the result of validation"""
    is_valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    record_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0


class CustomerDataValidator:
    """Validates customer data quality"""
    
    # Validation patterns
    EMAIL_PATTERN = re.compile(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )
    
    # US Phone number patterns (multiple formats)
    PHONE_PATTERNS = [
        re.compile(r'^\d{10}$'),  # 1234567890
        re.compile(r'^\d{3}-\d{3}-\d{4}$'),  # 123-456-7890
        re.compile(r'^\(\d{3}\)\s?\d{3}-\d{4}$'),  # (123) 456-7890
        re.compile(r'^\+1\d{10}$'),  # +11234567890
    ]
    
    ZIP_CODE_PATTERN = re.compile(r'^\d{5}(-\d{4})?$')  # 12345 or 12345-6789
    
    STATE_CODES = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    }
    
    VALID_CUSTOMER_TYPES = {'RETAIL', 'WHOLESALE', 'CORPORATE', 'INDIVIDUAL'}
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize validator with optional configuration
        
        Args:
            config: Configuration dictionary with validation settings
        """
        self.config = config or {}
        self.errors: List[ValidationError] = []
        self.warnings: List[str] = []
        
        # Configurable thresholds
        self.max_name_length = self.config.get('max_name_length', 50)
        self.max_address_length = self.config.get('max_address_length', 200)
        self.allow_null_phone = self.config.get('allow_null_phone', False)
        self.allow_null_email = self.config.get('allow_null_email', False)
        self.strict_mode = self.config.get('strict_mode', True)
        
    def validate_record(self, record: Dict) -> Tuple[bool, List[ValidationError]]:
        """
        Validate a single customer record
        
        Args:
            record: Dictionary containing customer data
            
        Returns:
            Tuple of (is_valid, list of errors)
        """
        record_errors = []
        record_id = record.get('CUSTOMER_ID', 'UNKNOWN')
        
        # Required field validations
        record_errors.extend(self._validate_required_fields(record))
        
        # Data type validations
        record_errors.extend(self._validate_data_types(record))
        
        # Format validations
        record_errors.extend(self._validate_formats(record))
        
        # Business rule validations
        record_errors.extend(self._validate_business_rules(record))
        
        # Length validations
        record_errors.extend(self._validate_lengths(record))
        
        is_valid = len(record_errors) == 0
        
        if not is_valid:
            logger.warning(
                f"Validation failed for customer {record_id}: "
                f"{len(record_errors)} errors found"
            )
        
        return is_valid, record_errors
    
    def _validate_required_fields(self, record: Dict) -> List[ValidationError]:
        """Validate required fields are present and not null"""
        errors = []
        record_id = record.get('CUSTOMER_ID', 'UNKNOWN')
        
        required_fields = ['CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME']
        
        if not self.allow_null_email:
            required_fields.append('EMAIL')
        
        if not self.allow_null_phone:
            required_fields.append('PHONE')
        
        for field in required_fields:
            value = record.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                errors.append(ValidationError(
                    record_id=record_id,
                    field_name=field,
                    error_type='NULL_VALUE',
                    error_message=f"Required field '{field}' is null or empty",
                    record_data=record
                ))
        
        return errors
    
    def _validate_data_types(self, record: Dict) -> List[ValidationError]:
        """Validate data types of fields"""
        errors = []
        record_id = record.get('CUSTOMER_ID', 'UNKNOWN')
        
        # String fields
        string_fields = [
            'CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME', 'EMAIL',
            'PHONE', 'ADDRESS', 'CITY', 'STATE', 'ZIP_CODE',
            'COUNTRY', 'CUSTOMER_TYPE'
        ]
        
        for field in string_fields:
            value = record.get(field)
            if value is not None and not isinstance(value, str):
                errors.append(ValidationError(
                    record_id=record_id,
                    field_name=field,
                    error_type='INVALID_TYPE',
                    error_message=f"Field '{field}' must be a string, got {type(value).__name__}",
                    record_data=record
                ))
        
        # Date fields
        if 'REGISTRATION_DATE' in record:
            reg_date = record['REGISTRATION_DATE']
            if reg_date is not None:
                if not isinstance(reg_date, (datetime, str)):
                    errors.append(ValidationError(
                        record_id=record_id,
                        field_name='REGISTRATION_DATE',
                        error_type='INVALID_TYPE',
                        error_message=f"REGISTRATION_DATE must be datetime or string, got {type(reg_date).__name__}",
                        record_data=record
                    ))
        
        return errors
    
    def _validate_formats(self, record: Dict) -> List[ValidationError]:
        """Validate field formats"""
        errors = []
        record_id = record.get('CUSTOMER_ID', 'UNKNOWN')
        
        # Email validation
        email = record.get('EMAIL')
        if email and isinstance(email, str):
            if not self.EMAIL_PATTERN.match(email.strip()):
                errors.append(ValidationError(
                    record_id=record_id,
                    field_name='EMAIL',
                    error_type='INVALID_FORMAT',
                    error_message=f"Invalid email format: '{email}'",
                    record_data=record
                ))
        
        # Phone validation
        phone = record.get('PHONE')
        if phone and isinstance(phone, str):
            phone_clean = phone.strip()
            if not any(pattern.match(phone_clean) for pattern in self.PHONE_PATTERNS):
                errors.append(ValidationError(
                    record_id=record_id,
                    field_name='PHONE',
                    error_type='INVALID_FORMAT',
                    error_message=f"Invalid phone number format: '{phone}'",
                    record_data=record
                ))
        
        # ZIP code validation
        zip_code = record.get('ZIP_CODE')
        if zip_code and isinstance(zip_code, str):
            if not self.ZIP_CODE_PATTERN.match(zip_code.strip()):
                errors.append(ValidationError(
                    record_id=record_id,
                    field_name='ZIP_CODE',
                    error_type='INVALID_FORMAT',
                    error_message=f"Invalid ZIP code format: '{zip_code}'",
                    record_data=record
                ))
        
        # State code validation
        state = record.get('STATE')
        if state and isinstance(state, str):
            state_upper = state.strip().upper()
            if len(state_upper) == 2 and state_upper not in self.STATE_CODES:
                errors.append(ValidationError(
                    record_id=record_id,
                    field_name='STATE',
                    error_type='INVALID_VALUE',
                    error_message=f"Invalid state code: '{state}'",
                    record_data=record
                ))
        
        return errors
    
    def _validate_business_rules(self, record: Dict) -> List[ValidationError]:
        """Validate business rules"""
        errors = []
        record_id = record.get('CUSTOMER_ID', 'UNKNOWN')
        
        # Customer type validation
        customer_type = record.get('CUSTOMER_TYPE')
        if customer_type and isinstance(customer_type, str):
            if customer_type.strip().upper() not in self.VALID_CUSTOMER_TYPES:
                errors.append(ValidationError(
                    record_id=record_id,
                    field_name='CUSTOMER_TYPE',
                    error_type='INVALID_VALUE',
                    error_message=f"Invalid customer type: '{customer_type}'. Must be one of {self.VALID_CUSTOMER_TYPES}",
                    record_data=record
                ))
        
        # Registration date validation
        reg_date = record.get('REGISTRATION_DATE')
        if reg_date:
            try:
                if isinstance(reg_date, str):
                    parsed_date = datetime.strptime(reg_date, '%Y-%m-%d')
                else:
                    parsed_date = reg_date
                
                if parsed_date > datetime.now():
                    errors.append(ValidationError(
                        record_id=record_id,
                        field_name='REGISTRATION_DATE',
                        error_type='INVALID_VALUE',
                        error_message=f"Registration date cannot be in the future: '{reg_date}'",
                        record_data=record
                    ))
            except ValueError:
                errors.append(ValidationError(
                    record_id=record_id,
                    field_name='REGISTRATION_DATE',
                    error_type='INVALID_FORMAT',
                    error_message=f"Invalid date format: '{reg_date}'",
                    record_data=record
                ))
        
        return errors
    
    def _validate_lengths(self, record: Dict) -> List[ValidationError]:
        """Validate field lengths"""
        errors = []
        record_id = record.get('CUSTOMER_ID', 'UNKNOWN')
        
        length_checks = {
            'CUSTOMER_ID': 20,
            'FIRST_NAME': self.max_name_length,
            'LAST_NAME': self.max_name_length,
            'EMAIL': 100,
            'PHONE': 20,
            'ADDRESS': self.max_address_length,
            'CITY': 50,
            'STATE': 2,
            'ZIP_CODE': 10,
            'COUNTRY': 50,
            'CUSTOMER_TYPE': 20
        }
        
        for field, max_length in length_checks.items():
            value = record.get(field)
            if value and isinstance(value, str):
                if len(value) > max_length:
                    errors.append(ValidationError(
                        record_id=record_id,
                        field_name=field,
                        error_type='LENGTH_EXCEEDED',
                        error_message=f"Field '{field}' exceeds maximum length of {max_length}: {len(value)} characters",
                        record_data=record
                    ))
        
        return errors
    
    def validate_batch(self, records: List[Dict]) -> ValidationResult:
        """
        Validate a batch of customer records
        
        Args:
            records: List of customer record dictionaries
            
        Returns:
            ValidationResult object with summary statistics
        """
        logger.info(f"Starting validation of {len(records)} customer records")
        
        all_errors = []
        valid_count = 0
        invalid_count = 0
        
        for i, record in enumerate(records):
            try:
                is_valid, errors = self.validate_record(record)
                
                if is_valid:
                    valid_count += 1
                else:
                    invalid_count += 1
                    all_errors.extend(errors)
                
                # Log progress every 1000 records
                if (i + 1) % 1000 == 0:
                    logger.info(f"Validated {i + 1}/{len(records)} records")
                    
            except Exception as e:
                logger.error(f"Unexpected error validating record {i}: {str(e)}")
                record_id = record.get('CUSTOMER_ID', f'INDEX_{i}')
                all_errors.append(ValidationError(
                    record_id=record_id,
                    field_name='UNKNOWN',
                    error_type='VALIDATION_ERROR',
                    error_message=f"Unexpected validation error: {str(e)}",
                    record_data=record
                ))
                invalid_count += 1
        
        result = ValidationResult(
            is_valid=(invalid_count == 0),
            errors=all_errors,
            record_count=len(records),
            valid_count=valid_count,
            invalid_count=invalid_count
        )
        
        logger.info(
            f"Validation complete: {valid_count} valid, "
            f"{invalid_count} invalid out of {len(records)} total records"
        )
        
        return result
    
    def get_error_summary(self, errors: List[ValidationError]) -> Dict:
        """
        Generate summary statistics for validation errors
        
        Args:
            errors: List of validation errors
            
        Returns:
            Dictionary with error statistics
        """
        if not errors:
            return {
                'total_errors': 0,
                'by_type': {},
                'by_field': {},
                'affected_records': 0
            }
        
        error_by_type = {}
        error_by_field = {}
        affected_records = set()
        
        for error in errors:
            # Count by error type
            error_by_type[error.error_type] = error_by_type.get(error.error_type, 0) + 1
            
            # Count by field
            error_by_field[error.field_name] = error_by_field.get(error.field_name, 0) + 1
            
            # Track affected records
            affected_records.add(error.record_id)
        
        return {
            'total_errors': len(errors),
            'by_type': error_by_type,
            'by_field': error_by_field,
            'affected_records': len(affected_records)
        }


def sanitize_customer_data(record: Dict) -> Dict:
    """
    Sanitize and clean customer data
    
    Args:
        record: Customer record dictionary
        
    Returns:
        Cleaned record dictionary
    """
    cleaned = record.copy()
    
    # Trim whitespace from string fields
    string_fields = [
        'CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME', 'EMAIL',
        'PHONE', 'ADDRESS', 'CITY', 'STATE', 'ZIP_CODE',
        'COUNTRY', 'CUSTOMER_TYPE'
    ]
    
    for field in string_fields:
        if field in cleaned and isinstance(cleaned[field], str):
            cleaned[field] = cleaned[field].strip()
    
    # Uppercase certain fields
    if 'STATE' in cleaned and cleaned['STATE']:
        cleaned['STATE'] = cleaned['STATE'].upper()
    
    if 'CUSTOMER_TYPE' in cleaned and cleaned['CUSTOMER_TYPE']:
        cleaned['CUSTOMER_TYPE'] = cleaned['CUSTOMER_TYPE'].upper()
    
    # Lowercase email
    if 'EMAIL' in cleaned and cleaned['EMAIL']:
        cleaned['EMAIL'] = cleaned['EMAIL'].lower()
    
    # Clean phone number (remove formatting)
    if 'PHONE' in cleaned and cleaned['PHONE']:
        phone = cleaned['PHONE']
        cleaned['PHONE_RAW'] = phone
        # Remove common formatting characters
        cleaned['PHONE'] = re.sub(r'[^\d+]', '', phone)
    
    return cleaned


if __name__ == '__main__':
    # Example usage
    sample_records = [
        {
            'CUSTOMER_ID': 'CUST001',
            'FIRST_NAME': 'John',
            'LAST_NAME': 'Doe',
            'EMAIL': 'john.doe@example.com',
            'PHONE': '123-456-7890',
            'ADDRESS': '123 Main St',
            'CITY': 'New York',
            'STATE': 'NY',
            'ZIP_CODE': '10001',
            'COUNTRY': 'USA',
            'REGISTRATION_DATE': '2023-01-15',
            'CUSTOMER_TYPE': 'RETAIL'
        },
        {
            'CUSTOMER_ID': 'CUST002',
            'FIRST_NAME': 'Jane',
            'LAST_NAME': None,  # Missing last name
            'EMAIL': 'invalid-email',  # Invalid email
            'PHONE': '1234',  # Invalid phone
            'ADDRESS': '456 Oak Ave',
            'CITY': 'Los Angeles',
            'STATE': 'ZZ',  # Invalid state
            'ZIP_CODE': '90001',
            'COUNTRY': 'USA',
            'REGISTRATION_DATE': '2025-12-31',  # Future date
            'CUSTOMER_TYPE': 'INVALID_TYPE'
        }
    ]
    
    validator = CustomerDataValidator()
    result = validator.validate_batch(sample_records)
    
    print(f"\nValidation Results:")
    print(f"Total Records: {result.record_count}")
    print(f"Valid: {result.valid_count}")
    print(f"Invalid: {result.invalid_count}")
    print(f"\nError Summary:")
    summary = validator.get_error_summary(result.errors)
    print(f"Total Errors: {summary['total_errors']}")
    print(f"By Type: {summary['by_type']}")
    print(f"By Field: {summary['by_field']}")