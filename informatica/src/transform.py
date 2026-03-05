"""
Transform module for Full Refresh workflow
Handles data cleansing, SCD logic, and dimension processing
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import yaml

logger = logging.getLogger(__name__)


class TransformProcessor:
    """Handles data transformations and business logic"""
    
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.warehouse_config = self.config['database']['warehouse']
    
    def cleanse_customer_data(self, record: Dict) -> Dict:
        """
        Cleanse and standardize customer data
        Replicates EXP_CLEANSE_CUSTOMER transformation
        """
        cleansed = {}
        
        # Basic field passthrough
        cleansed['CUSTOMER_ID'] = record.get('CUSTOMER_ID', '')
        
        # Name cleansing
        first_name = record.get('FIRST_NAME', '').strip().upper()
        last_name = record.get('LAST_NAME', '').strip().upper()
        cleansed['FIRST_NAME_CLEAN'] = first_name
        cleansed['LAST_NAME_CLEAN'] = last_name
        cleansed['FULL_NAME'] = f"{first_name} {last_name}"
        
        # Email standardization
        cleansed['EMAIL_CLEAN'] = record.get('EMAIL', '').strip().lower()
        
        # Phone cleansing - remove formatting characters
        phone = record.get('PHONE', '')
        cleansed['PHONE_CLEAN'] = ''.join(c for c in phone if c.isdigit())
        
        # Address standardization
        address = record.get('ADDRESS', '').strip()
        cleansed['ADDRESS_CLEAN'] = address.title() if address else ''
        
        # Location fields
        cleansed['CITY_CLEAN'] = record.get('CITY', '').strip().upper()
        cleansed['STATE_CLEAN'] = record.get('STATE', '').strip().upper()
        cleansed['ZIP_CODE_CLEAN'] = record.get('ZIP_CODE', '').strip()
        cleansed['COUNTRY_CLEAN'] = record.get('COUNTRY', '').strip().upper()
        
        # Customer type
        cleansed['CUSTOMER_TYPE_CLEAN'] = record.get('CUSTOMER_TYPE', '').strip().upper()
        
        # Timestamps
        cleansed['REGISTRATION_DATE'] = record.get('REGISTRATION_DATE')
        cleansed['CURRENT_TIMESTAMP'] = datetime.now()
        
        return cleansed
    
    def cleanse_product_data(self, record: Dict) -> Dict:
        """
        Cleanse and standardize product data
        Replicates EXP_CLEANSE_PRODUCT transformation
        """
        cleansed = {}
        
        # Basic fields
        cleansed['PRODUCT_ID'] = record.get('PRODUCT_ID', '')
        
        # Text field standardization
        product_name = record.get('PRODUCT_NAME', '').strip()
        cleansed['PRODUCT_NAME_CLEAN'] = product_name.title() if product_name else ''
        
        cleansed['PRODUCT_DESC_CLEAN'] = record.get('PRODUCT_DESCRIPTION', '').strip()
        cleansed['CATEGORY_CLEAN'] = record.get('CATEGORY', '').strip().upper()
        cleansed['SUB_CATEGORY_CLEAN'] = record.get('SUB_CATEGORY', '').strip().upper()
        
        brand = record.get('BRAND', '').strip()
        cleansed['BRAND_CLEAN'] = brand.title() if brand else ''
        
        # Numeric fields - round to 2 decimals
        try:
            unit_price = float(record.get('UNIT_PRICE', 0))
            cost_price = float(record.get('COST_PRICE', 0))
            cleansed['UNIT_PRICE_CLEAN'] = round(unit_price, 2)
            cleansed['COST_PRICE_CLEAN'] = round(cost_price, 2)
            
            # Calculate profit margin
            if unit_price > 0:
                margin = ((unit_price - cost_price) / unit_price) * 100
                cleansed['PROFIT_MARGIN'] = round(margin, 2)
            else:
                cleansed['PROFIT_MARGIN'] = 0.0
            
            # Price range categorization
            if unit_price < 50:
                cleansed['PRICE_RANGE'] = 'LOW'
            elif unit_price < 200:
                cleansed['PRICE_RANGE'] = 'MEDIUM'
            elif unit_price < 500:
                cleansed['PRICE_RANGE'] = 'HIGH'
            else:
                cleansed['PRICE_RANGE'] = 'PREMIUM'
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Error processing numeric fields: {e}")
            cleansed['UNIT_PRICE_CLEAN'] = 0.0
            cleansed['COST_PRICE_CLEAN'] = 0.0
            cleansed['PROFIT_MARGIN'] = 0.0
            cleansed['PRICE_RANGE'] = 'UNKNOWN'
        
        # Supplier info
        supplier_name = record.get('SUPPLIER_NAME', '').strip()
        cleansed['SUPPLIER_NAME_CLEAN'] = supplier_name.title() if supplier_name else ''
        cleansed['SUPPLIER_ID'] = record.get('SUPPLIER_ID', '')
        
        # Physical attributes
        cleansed['WEIGHT'] = record.get('WEIGHT')
        cleansed['DIMENSIONS'] = record.get('DIMENSIONS', '')
        cleansed['COLOR'] = record.get('COLOR', '')
        cleansed['SIZE'] = record.get('SIZE', '')
        cleansed['MATERIAL'] = record.get('MATERIAL', '')
        
        # Status
        cleansed['STATUS_CLEAN'] = record.get('STATUS', '').strip().upper()
        
        cleansed['CURRENT_TIMESTAMP'] = datetime.now()
        
        return cleansed
    
    def calculate_sales_measures(self, record: Dict, lookup_data: Dict) -> Dict:
        """
        Calculate derived measures for sales fact
        Replicates EXP_CALCULATE_MEASURES transformation
        """
        calculated = {}
        
        # Basic fields
        calculated['TRANSACTION_ID'] = record.get('TRANSACTION_ID')
        calculated['TRANSACTION_DATE'] = record.get('TRANSACTION_DATE')
        calculated['CUSTOMER_ID'] = record.get('CUSTOMER_ID')
        calculated['PRODUCT_ID'] = record.get('PRODUCT_ID')
        
        # Get values
        try:
            quantity = int(record.get('QUANTITY', 0))
            unit_price = float(record.get('UNIT_PRICE', 0))
            discount_pct = float(record.get('DISCOUNT_PERCENT', 0))
            tax_amount = float(record.get('TAX_AMOUNT', 0))
            total_amount = float(record.get('TOTAL_AMOUNT', 0))
            cost_price = float(lookup_data.get('COST_PRICE', 0))
            
            # Calculate discount amount
            calculated['DISCOUNT_AMOUNT'] = round((unit_price * quantity * discount_pct / 100), 2)
            
            # Calculate cost amount
            calculated['COST_AMOUNT'] = round((cost_price * quantity), 2)
            
            # Calculate profit
            calculated['PROFIT_AMOUNT'] = round((total_amount - tax_amount - calculated['COST_AMOUNT']), 2)
            
            # Calculate profit margin
            net_revenue = total_amount - tax_amount
            if net_revenue > 0:
                profit_margin = (calculated['PROFIT_AMOUNT'] / net_revenue) * 100
                calculated['PROFIT_MARGIN_PERCENT'] = round(profit_margin, 2)
            else:
                calculated['PROFIT_MARGIN_PERCENT'] = 0.0
            
            # Pass through other fields
            calculated['QUANTITY'] = quantity
            calculated['UNIT_PRICE'] = unit_price
            calculated['DISCOUNT_PERCENT'] = discount_pct
            calculated['TAX_AMOUNT'] = tax_amount
            calculated['TOTAL_AMOUNT'] = total_amount
            
        except (ValueError, TypeError) as e:
            logger.error(f"Error calculating measures: {e}")
            # Set defaults on error
            calculated['DISCOUNT_AMOUNT'] = 0.0
            calculated['COST_AMOUNT'] = 0.0
            calculated['PROFIT_AMOUNT'] = 0.0
            calculated['PROFIT_MARGIN_PERCENT'] = 0.0
        
        # Additional fields
        calculated['PAYMENT_METHOD'] = record.get('PAYMENT_METHOD', '')
        calculated['STORE_ID'] = record.get('STORE_ID', '')
        calculated['REGION'] = record.get('REGION', '')
        calculated['LOAD_TIMESTAMP'] = datetime.now()
        
        # Calculate date key (YYYYMMDD format)
        trans_date = record.get('TRANSACTION_DATE')
        if trans_date:
            if isinstance(trans_date, str):
                trans_date = datetime.fromisoformat(trans_date.replace('Z', '+00:00'))
            calculated['DATE_KEY'] = int(trans_date.strftime('%Y%m%d'))
        
        return calculated
    
    def apply_scd_type2_logic(self, source_record: Dict, lookup_record: Optional[Dict]) -> Dict:
        """
        Apply SCD Type 2 logic for customer dimension
        Replicates EXP_SCD_LOGIC transformation
        """
        scd_result = {}
        
        # Determine if this is a new record
        is_new = lookup_record is None or not lookup_record
        scd_result['IS_NEW_RECORD'] = 1 if is_new else 0
        
        # Check for changes if existing record
        if is_new:
            scd_result['IS_CHANGED'] = 0
        else:
            # Compare key fields
            fields_changed = (
                source_record.get('FIRST_NAME_CLEAN') != lookup_record.get('FIRST_NAME') or
                source_record.get('LAST_NAME_CLEAN') != lookup_record.get('LAST_NAME') or
                source_record.get('EMAIL_CLEAN') != lookup_record.get('EMAIL') or
                source_record.get('PHONE_CLEAN') != lookup_record.get('PHONE') or
                source_record.get('ADDRESS_CLEAN') != lookup_record.get('ADDRESS')
            )
            scd_result['IS_CHANGED'] = 1 if fields_changed else 0
        
        # Set effective dates
        scd_result['EFFECTIVE_FROM'] = datetime.now()
        scd_result['EFFECTIVE_TO'] = datetime(9999, 12, 31)
        scd_result['IS_CURRENT_FLAG'] = 'Y'
        
        # Merge source data
        scd_result.update(source_record)
        
        # Add customer key from lookup or mark for sequence generation
        if not is_new and scd_result['IS_CHANGED'] == 0:
            scd_result['CUSTOMER_KEY'] = lookup_record.get('CUSTOMER_KEY')
        else:
            scd_result['CUSTOMER_KEY'] = None  # Will be generated
        
        return scd_result
    
    def apply_scd_type1_logic(self, source_record: Dict, lookup_record: Optional[Dict]) -> Dict:
        """
        Apply SCD Type 1 logic for product dimension
        Replicates EXP_SCD_TYPE1 transformation
        """
        scd_result = {}
        
        # Determine if new record
        is_new = lookup_record is None or not lookup_record
        scd_result['IS_NEW_RECORD'] = 1 if is_new else 0
        
        # Check for changes
        if is_new:
            scd_result['IS_CHANGED'] = 0
        else:
            fields_changed = (
                source_record.get('PRODUCT_NAME_CLEAN') != lookup_record.get('PRODUCT_NAME') or
                source_record.get('CATEGORY_CLEAN') != lookup_record.get('CATEGORY') or
                source_record.get('UNIT_PRICE_CLEAN') != lookup_record.get('UNIT_PRICE') or
                source_record.get('STATUS_CLEAN') != lookup_record.get('STATUS')
            )
            scd_result['IS_CHANGED'] = 1 if fields_changed else 0
        
        # Set update flag
        scd_result['UPDATE_FLAG'] = 1 if scd_result['IS_CHANGED'] == 1 else 0
        
        # Merge source data
        scd_result.update(source_record)
        
        # Handle product key
        if is_new:
            scd_result['PRODUCT_KEY'] = None  # Will be generated
        else:
            scd_result['PRODUCT_KEY'] = lookup_record.get('PRODUCT_KEY')
        
        scd_result['UPDATED_DATE'] = datetime.now() if scd_result['IS_CHANGED'] == 1 else None
        
        return scd_result
    
    def validate_record(self, record: Dict, entity_type: str) -> bool:
        """
        Validate record based on data quality rules
        Replicates FIL_VALID_* filters
        """
        if entity_type == 'CUSTOMER':
            return (
                record.get('CUSTOMER_ID') is not None and
                record.get('FIRST_NAME_CLEAN') is not None and
                record.get('LAST_NAME_CLEAN') is not None and
                record.get('EMAIL_CLEAN') is not None
            )
        
        elif entity_type == 'PRODUCT':
            return (
                record.get('PRODUCT_ID') is not None and
                record.get('PRODUCT_NAME_CLEAN') is not None and
                record.get('CATEGORY_CLEAN') is not None and
                float(record.get('UNIT_PRICE_CLEAN', -1)) >= 0 and
                float(record.get('COST_PRICE_CLEAN', -1)) >= 0
            )
        
        elif entity_type == 'SALES':
            return (
                record.get('CUSTOMER_KEY') is not None and
                record.get('PRODUCT_KEY') is not None and
                int(record.get('QUANTITY', 0)) > 0 and
                float(record.get('TOTAL_AMOUNT', 0)) > 0
            )
        
        return False


def get_lookup_sql(entity_type: str) -> str:
    """Generate lookup SQL for dimension tables"""
    
    if entity_type == 'CUSTOMER':
        return """
        SELECT 
            CUSTOMER_KEY,
            CUSTOMER_ID,
            FIRST_NAME,
            LAST_NAME,
            EMAIL,
            PHONE,
            ADDRESS,
            EFFECTIVE_FROM_DATE
        FROM DWH.DIM_CUSTOMER
        WHERE CUSTOMER_ID = ? 
        AND IS_CURRENT = 'Y'
        """
    
    elif entity_type == 'PRODUCT':
        return """
        SELECT 
            PRODUCT_KEY,
            PRODUCT_ID,
            PRODUCT_NAME,
            CATEGORY,
            UNIT_PRICE,
            STATUS,
            COST_PRICE,
            UPDATED_DATE
        FROM DWH.DIM_PRODUCT
        WHERE PRODUCT_ID = ?
        """
    
    return ""


if __name__ == "__main__":
    # Test transformations
    logging.basicConfig(level=logging.INFO)
    
    transformer = TransformProcessor()
    
    # Test customer cleansing
    sample_customer = {
        'CUSTOMER_ID': 'C001',
        'FIRST_NAME': '  john  ',
        'LAST_NAME': 'DOE',
        'EMAIL': 'John.Doe@Example.COM',
        'PHONE': '(555) 123-4567',
        'ADDRESS': '123 main street',
        'CITY': 'new york',
        'STATE': 'ny',
        'ZIP_CODE': '10001',
        'COUNTRY': 'usa',
        'CUSTOMER_TYPE': 'premium'
    }
    
    cleansed = transformer.cleanse_customer_data(sample_customer)
    print("Cleansed Customer Data:")
    for key, value in cleansed.items():
        print(f"  {key}: {value}")