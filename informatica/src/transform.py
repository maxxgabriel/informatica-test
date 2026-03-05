"""
Transform module for Product Dimension ETL
Implements data cleansing, price rounding, and profit margin calculations
"""
import logging
from typing import List, Dict, Any, Optional
from decimal import Decimal, ROUND_HALF_UP
import re

logger = logging.getLogger(__name__)


class ProductTransformer:
    """Transform product data with cleansing and calculations"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transformer with configuration
        
        Args:
            config: Transformation rules and parameters
        """
        self.config = config
        self.transform_config = config.get('transform', {})
        self.stats = {
            'total_processed': 0,
            'total_transformed': 0,
            'total_rejected': 0,
            'validation_errors': []
        }
        
    def transform_products(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform list of product records
        
        Args:
            products: List of raw product records
            
        Returns:
            List of transformed product records
        """
        transformed = []
        
        for product in products:
            self.stats['total_processed'] += 1
            
            try:
                transformed_product = self.transform_product(product)
                
                if self.validate_product(transformed_product):
                    transformed.append(transformed_product)
                    self.stats['total_transformed'] += 1
                else:
                    self.stats['total_rejected'] += 1
                    logger.warning(f"Product rejected: {product.get('PRODUCT_ID', 'Unknown')}")
                    
            except Exception as e:
                self.stats['total_rejected'] += 1
                error_msg = f"Error transforming product {product.get('PRODUCT_ID', 'Unknown')}: {e}"
                logger.error(error_msg)
                self.stats['validation_errors'].append(error_msg)
                
        logger.info(f"Transformation complete: {self.stats['total_transformed']} successful, "
                   f"{self.stats['total_rejected']} rejected")
        
        return transformed
        
    def transform_product(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform single product record
        
        Args:
            product: Raw product record
            
        Returns:
            Transformed product record
        """
        transformed = {}
        
        # Copy product ID (key field)
        transformed['PRODUCT_ID'] = product.get('PRODUCT_ID')
        
        # Text cleansing transformations
        transformed['PRODUCT_NAME_CLEAN'] = self.cleanse_product_name(
            product.get('PRODUCT_NAME')
        )
        transformed['PRODUCT_DESC_CLEAN'] = self.cleanse_description(
            product.get('PRODUCT_DESCRIPTION')
        )
        transformed['CATEGORY_CLEAN'] = self.cleanse_category(
            product.get('CATEGORY')
        )
        transformed['SUB_CATEGORY_CLEAN'] = self.cleanse_category(
            product.get('SUB_CATEGORY')
        )
        transformed['BRAND_CLEAN'] = self.cleanse_brand(
            product.get('BRAND')
        )
        
        # Price transformations with 2 decimal rounding
        transformed['UNIT_PRICE_CLEAN'] = self.round_price(
            product.get('UNIT_PRICE')
        )
        transformed['COST_PRICE_CLEAN'] = self.round_price(
            product.get('COST_PRICE')
        )
        
        # Supplier information
        transformed['SUPPLIER_ID'] = product.get('SUPPLIER_ID')
        transformed['SUPPLIER_NAME_CLEAN'] = self.cleanse_supplier_name(
            product.get('SUPPLIER_NAME')
        )
        
        # Physical attributes
        transformed['WEIGHT'] = product.get('WEIGHT')
        transformed['DIMENSIONS'] = product.get('DIMENSIONS')
        transformed['COLOR'] = self.cleanse_text_field(product.get('COLOR'))
        transformed['SIZE'] = self.cleanse_text_field(product.get('SIZE'))
        transformed['MATERIAL'] = self.cleanse_text_field(product.get('MATERIAL'))
        
        # Status
        transformed['STATUS_CLEAN'] = self.cleanse_status(
            product.get('STATUS')
        )
        
        # Calculate profit margin
        transformed['PROFIT_MARGIN'] = self.calculate_profit_margin(
            transformed['UNIT_PRICE_CLEAN'],
            transformed['COST_PRICE_CLEAN']
        )
        
        # Calculate price range category
        transformed['PRICE_RANGE'] = self.calculate_price_range(
            transformed['UNIT_PRICE_CLEAN']
        )
        
        # Metadata
        transformed['SOURCE_SYSTEM'] = product.get('SOURCE_SYSTEM', 'CSV_FILE')
        transformed['LOAD_DATE'] = product.get('LOAD_DATE')
        
        return transformed
        
    def cleanse_product_name(self, value: Optional[str]) -> Optional[str]:
        """
        Cleanse product name: trim, title case
        
        Args:
            value: Raw product name
            
        Returns:
            Cleansed product name
        """
        if not value:
            return None
        
        # Trim whitespace
        cleaned = value.strip()
        
        # Title case (Initcap equivalent)
        cleaned = cleaned.title()
        
        # Remove multiple spaces
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        return cleaned if cleaned else None
        
    def cleanse_description(self, value: Optional[str]) -> Optional[str]:
        """
        Cleanse product description: trim only
        
        Args:
            value: Raw description
            
        Returns:
            Cleansed description
        """
        if not value:
            return None
        
        cleaned = value.strip()
        return cleaned if cleaned else None
        
    def cleanse_category(self, value: Optional[str]) -> Optional[str]:
        """
        Cleanse category: uppercase and trim
        
        Args:
            value: Raw category
            
        Returns:
            Cleansed category
        """
        if not value:
            return None
        
        cleaned = value.strip().upper()
        return cleaned if cleaned else None
        
    def cleanse_brand(self, value: Optional[str]) -> Optional[str]:
        """
        Cleanse brand name: title case and trim
        
        Args:
            value: Raw brand name
            
        Returns:
            Cleansed brand name
        """
        if not value:
            return None
        
        cleaned = value.strip().title()
        return cleaned if cleaned else None
        
    def cleanse_supplier_name(self, value: Optional[str]) -> Optional[str]:
        """
        Cleanse supplier name: title case and trim
        
        Args:
            value: Raw supplier name
            
        Returns:
            Cleansed supplier name
        """
        if not value:
            return None
        
        cleaned = value.strip().title()
        return cleaned if cleaned else None
        
    def cleanse_status(self, value: Optional[str]) -> Optional[str]:
        """
        Cleanse status: uppercase
        
        Args:
            value: Raw status
            
        Returns:
            Cleansed status
        """
        if not value:
            return None
        
        return value.strip().upper()
        
    def cleanse_text_field(self, value: Optional[str]) -> Optional[str]:
        """
        Generic text field cleansing: trim
        
        Args:
            value: Raw text value
            
        Returns:
            Cleansed text value
        """
        if not value:
            return None
        
        cleaned = value.strip()
        return cleaned if cleaned else None
        
    def round_price(self, value: Any) -> Optional[Decimal]:
        """
        Round price to 2 decimal places
        
        Args:
            value: Raw price value
            
        Returns:
            Rounded price as Decimal
        """
        if value is None:
            return None
        
        try:
            # Convert to Decimal for precise rounding
            price = Decimal(str(value))
            
            # Round to 2 decimal places
            rounded = price.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            return rounded
            
        except Exception as e:
            logger.warning(f"Error rounding price {value}: {e}")
            return None
            
    def calculate_profit_margin(self, unit_price: Optional[Decimal], 
                               cost_price: Optional[Decimal]) -> Optional[Decimal]:
        """
        Calculate profit margin percentage
        Formula: ((unit_price - cost_price) / unit_price) * 100
        
        Args:
            unit_price: Unit selling price
            cost_price: Cost price
            
        Returns:
            Profit margin percentage rounded to 2 decimals
        """
        if unit_price is None or cost_price is None:
            return Decimal('0.00')
        
        try:
            # Avoid division by zero
            if unit_price <= 0:
                return Decimal('0.00')
            
            # Calculate margin
            margin = ((unit_price - cost_price) / unit_price) * Decimal('100')
            
            # Round to 2 decimal places
            margin_rounded = margin.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            return margin_rounded
            
        except Exception as e:
            logger.warning(f"Error calculating profit margin: {e}")
            return Decimal('0.00')
            
    def calculate_price_range(self, unit_price: Optional[Decimal]) -> str:
        """
        Calculate price range category
        
        Args:
            unit_price: Unit selling price
            
        Returns:
            Price range category: LOW, MEDIUM, HIGH, PREMIUM
        """
        if unit_price is None:
            return 'UNKNOWN'
        
        try:
            price_ranges = self.transform_config.get('price_ranges', {
                'low': 50,
                'medium': 200,
                'high': 500
            })
            
            if unit_price < price_ranges['low']:
                return 'LOW'
            elif unit_price < price_ranges['medium']:
                return 'MEDIUM'
            elif unit_price < price_ranges['high']:
                return 'HIGH'
            else:
                return 'PREMIUM'
                
        except Exception as e:
            logger.warning(f"Error calculating price range: {e}")
            return 'UNKNOWN'
            
    def validate_product(self, product: Dict[str, Any]) -> bool:
        """
        Validate transformed product record
        
        Args:
            product: Transformed product record
            
        Returns:
            True if valid, False otherwise
        """
        validations = self.transform_config.get('validations', {})
        
        # Required fields check
        required_fields = validations.get('required_fields', [
            'PRODUCT_ID',
            'PRODUCT_NAME_CLEAN',
            'CATEGORY_CLEAN'
        ])
        
        for field in required_fields:
            if not product.get(field):
                logger.warning(f"Validation failed: missing {field}")
                return False
        
        # Price validations
        if product.get('UNIT_PRICE_CLEAN') is not None:
            if product['UNIT_PRICE_CLEAN'] < 0:
                logger.warning("Validation failed: negative unit price")
                return False
        
        if product.get('COST_PRICE_CLEAN') is not None:
            if product['COST_PRICE_CLEAN'] < 0:
                logger.warning("Validation failed: negative cost price")
                return False
        
        return True
        
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get transformation statistics
        
        Returns:
            Statistics dictionary
        """
        return self.stats.copy()


def transform_product_data(products: List[Dict[str, Any]], 
                          config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Main function to transform product data
    
    Args:
        products: List of raw product records
        config: Configuration dictionary
        
    Returns:
        List of transformed product records
    """
    transformer = ProductTransformer(config)
    transformed = transformer.transform_products(products)
    
    stats = transformer.get_statistics()
    logger.info(f"Transformation statistics: {stats}")
    
    return transformed


if __name__ == "__main__":
    # Test transformation
    import yaml
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Sample data
    sample_products = [
        {
            'PRODUCT_ID': 'P001',
            'PRODUCT_NAME': '  wireless mouse  ',
            'PRODUCT_DESCRIPTION': '  Ergonomic wireless mouse  ',
            'CATEGORY': 'electronics',
            'SUB_CATEGORY': 'computer accessories',
            'BRAND': 'logitech',
            'UNIT_PRICE': 29.999,
            'COST_PRICE': 15.50,
            'STATUS': 'active'
        }
    ]
    
    transformed = transform_product_data(sample_products, config)
    print(f"Transformed {len(transformed)} products")
    for product in transformed:
        print(product)