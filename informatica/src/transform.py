"""
Transform module for m_LOAD_STG_PRODUCT
Applies data transformations and adds metadata fields
"""
import logging
from typing import Dict, Any, Generator
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import nipyapi

logger = logging.getLogger(__name__)


class ProductTransformer:
    """Transform product data and add staging metadata"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transformer with configuration
        
        Args:
            config: Configuration dictionary with transformation settings
        """
        self.config = config
        self.source_system = config['staging']['source_system']
        self.record_id_start = config['staging'].get('record_id_start', 1)
        self.current_record_id = self.record_id_start
        
    def transform(self, records: Generator[Dict[str, Any], None, None]) -> Generator[Dict[str, Any], None, None]:
        """
        Transform product records and add metadata
        
        Args:
            records: Generator of extracted product records
            
        Yields:
            Transformed records with metadata
        """
        logger.info("Starting transformation process")
        transform_count = 0
        error_count = 0
        
        for record in records:
            try:
                transformed = self._transform_record(record)
                transform_count += 1
                yield transformed
            except Exception as e:
                error_count += 1
                logger.error(f"Error transforming record {record.get('PRODUCT_ID')}: {e}")
                continue
                
        logger.info(f"Transformation complete. Processed: {transform_count}, Errors: {error_count}")
    
    def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform individual product record
        
        Args:
            record: Source record dictionary
            
        Returns:
            Transformed record with metadata and cleansed fields
        """
        # Add metadata fields
        transformed = {
            'RECORD_ID': self._get_next_record_id(),
            'LOAD_DATE': datetime.now(),
            'SOURCE_SYSTEM': self.source_system
        }
        
        # Apply data cleansing transformations
        transformed['PRODUCT_ID'] = self._cleanse_product_id(record['PRODUCT_ID'])
        transformed['PRODUCT_NAME'] = self._cleanse_text(record['PRODUCT_NAME'], capitalize='title')
        transformed['PRODUCT_DESCRIPTION'] = self._cleanse_text(record['PRODUCT_DESCRIPTION'])
        transformed['CATEGORY'] = self._cleanse_text(record['CATEGORY'], capitalize='upper')
        transformed['SUB_CATEGORY'] = self._cleanse_text(record['SUB_CATEGORY'], capitalize='upper')
        transformed['BRAND'] = self._cleanse_text(record['BRAND'], capitalize='title')
        
        # Numeric field transformations
        transformed['UNIT_PRICE'] = self._round_decimal(record['UNIT_PRICE'], 2)
        transformed['COST_PRICE'] = self._round_decimal(record['COST_PRICE'], 2)
        transformed['WEIGHT'] = self._round_decimal(record['WEIGHT'], 2)
        
        # Other fields
        transformed['SUPPLIER_ID'] = self._cleanse_text(record['SUPPLIER_ID'])
        transformed['SUPPLIER_NAME'] = self._cleanse_text(record['SUPPLIER_NAME'], capitalize='title')
        transformed['DIMENSIONS'] = self._cleanse_text(record['DIMENSIONS'])
        transformed['COLOR'] = self._cleanse_text(record['COLOR'], capitalize='title')
        transformed['SIZE'] = self._cleanse_text(record['SIZE'], capitalize='upper')
        transformed['MATERIAL'] = self._cleanse_text(record['MATERIAL'], capitalize='title')
        transformed['STATUS'] = self._cleanse_text(record['STATUS'], capitalize='upper')
        
        # Calculate derived fields
        transformed['PROFIT_MARGIN'] = self._calculate_profit_margin(
            transformed['UNIT_PRICE'], 
            transformed['COST_PRICE']
        )
        transformed['PRICE_RANGE'] = self._calculate_price_range(transformed['UNIT_PRICE'])
        
        # Add audit timestamp
        transformed['CREATED_TIMESTAMP'] = datetime.now()
        
        return transformed
    
    def _get_next_record_id(self) -> int:
        """
        Get next record ID in sequence
        
        Returns:
            Next record ID
        """
        record_id = self.current_record_id
        self.current_record_id += 1
        return record_id
    
    @staticmethod
    def _cleanse_text(text: str, capitalize: str = None) -> str:
        """
        Cleanse text field - trim and optionally capitalize
        
        Args:
            text: Input text
            capitalize: Capitalization mode ('upper', 'lower', 'title', None)
            
        Returns:
            Cleansed text
        """
        if not text:
            return ''
            
        cleansed = text.strip()
        
        if capitalize == 'upper':
            cleansed = cleansed.upper()
        elif capitalize == 'lower':
            cleansed = cleansed.lower()
        elif capitalize == 'title':
            cleansed = cleansed.title()
            
        return cleansed
    
    @staticmethod
    def _cleanse_product_id(product_id: str) -> str:
        """
        Cleanse product ID - uppercase and trim
        
        Args:
            product_id: Input product ID
            
        Returns:
            Cleansed product ID
        """
        return product_id.strip().upper()
    
    @staticmethod
    def _round_decimal(value: float, places: int) -> Decimal:
        """
        Round decimal to specified places
        
        Args:
            value: Numeric value
            places: Decimal places
            
        Returns:
            Rounded Decimal value
        """
        if value is None:
            return Decimal('0')
            
        decimal_value = Decimal(str(value))
        quantizer = Decimal(10) ** -places
        return decimal_value.quantize(quantizer, rounding=ROUND_HALF_UP)
    
    @staticmethod
    def _calculate_profit_margin(unit_price: Decimal, cost_price: Decimal) -> Decimal:
        """
        Calculate profit margin percentage
        
        Args:
            unit_price: Product unit price
            cost_price: Product cost price
            
        Returns:
            Profit margin percentage
        """
        if unit_price <= 0:
            return Decimal('0')
            
        margin = ((unit_price - cost_price) / unit_price) * 100
        return margin.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    
    @staticmethod
    def _calculate_price_range(unit_price: Decimal) -> str:
        """
        Calculate price range category
        
        Args:
            unit_price: Product unit price
            
        Returns:
            Price range category (LOW, MEDIUM, HIGH, PREMIUM)
        """
        price = float(unit_price)
        
        if price < 50:
            return 'LOW'
        elif price < 200:
            return 'MEDIUM'
        elif price < 500:
            return 'HIGH'
        else:
            return 'PREMIUM'


class NiFiProductTransformer:
    """NiFi-based transformer using UpdateAttribute and JoltTransform processors"""
    
    def __init__(self, config: Dict[str, Any], canvas: Any):
        """
        Initialize NiFi transformer
        
        Args:
            config: Configuration dictionary
            canvas: NiFi canvas object (process group)
        """
        self.config = config
        self.canvas = canvas
        
    def create_transform_flow(self, source_processor_id: str) -> Dict[str, Any]:
        """
        Create NiFi flow for product transformation
        
        Args:
            source_processor_id: ID of upstream processor to connect from
            
        Returns:
            Dictionary with created processor IDs
        """
        logger.info("Creating NiFi transform flow")
        
        source_processor = nipyapi.canvas.get_processor(source_processor_id, 'id')
        
        # Create UpdateAttribute for metadata
        update_metadata = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.attributes.UpdateAttribute'),
            location=(900, 100),
            name='AddMetadata',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'LOAD_DATE': "${now():format('yyyy-MM-dd HH:mm:ss')}",
                    'SOURCE_SYSTEM': self.config['staging']['source_system'],
                    'RECORD_ID': "${nextInt()}"
                }
            )
        )
        
        # Create UpdateAttribute for data cleansing
        cleanse_data = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.attributes.UpdateAttribute'),
            location=(1100, 100),
            name='CleanseFields',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'PRODUCT_NAME': "${product.name:trim():toUpper()}",
                    'CATEGORY': "${product.category:trim():toUpper()}",
                    'STATUS': "${product.status:trim():toUpper()}",
                    'UNIT_PRICE': "${product.price:trim()}",
                    'COST_PRICE': "${product.cost:trim()}"
                }
            )
        )
        
        # Create JoltTransformJSON for complex transformations
        jolt_spec = {
            "operation": "shift",
            "spec": {
                "PRODUCT_ID": "PRODUCT_ID",
                "PRODUCT_NAME": "PRODUCT_NAME",
                "PRODUCT_DESCRIPTION": "PRODUCT_DESCRIPTION",
                "CATEGORY": "CATEGORY",
                "SUB_CATEGORY": "SUB_CATEGORY",
                "BRAND": "BRAND",
                "UNIT_PRICE": "UNIT_PRICE",
                "COST_PRICE": "COST_PRICE",
                "SUPPLIER_ID": "SUPPLIER_ID",
                "SUPPLIER_NAME": "SUPPLIER_NAME",
                "WEIGHT": "WEIGHT",
                "DIMENSIONS": "DIMENSIONS",
                "COLOR": "COLOR",
                "SIZE": "SIZE",
                "MATERIAL": "MATERIAL",
                "STATUS": "STATUS",
                "LOAD_DATE": "LOAD_DATE",
                "SOURCE_SYSTEM": "SOURCE_SYSTEM",
                "RECORD_ID": "RECORD_ID"
            }
        }
        
        jolt_transform = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.JoltTransformJSON'),
            location=(1300, 100),
            name='TransformToStaging',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Jolt Specification': str(jolt_spec),
                    'Jolt Transform': 'jolt-transform-shift'
                },
                auto_terminated_relationships=['failure']
            )
        )
        
        # Create EvaluateJsonPath for derived calculations
        evaluate_json = nipyapi.canvas.create_processor(
            parent_pg=self.canvas,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.EvaluateJsonPath'),
            location=(1500, 100),
            name='CalculateDerivedFields',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Destination': 'flowfile-attribute',
                    'PROFIT_MARGIN': '$[?($.UNIT_PRICE > 0)].divide(subtract($.UNIT_PRICE, $.COST_PRICE), $.UNIT_PRICE)',
                    'PRICE_RANGE': '${literal("LOW"):ifElse(${UNIT_PRICE:lt(50)}, ${literal("MEDIUM"):ifElse(${UNIT_PRICE:lt(200)}, ${literal("HIGH"):ifElse(${UNIT_PRICE:lt(500)}, "PREMIUM")})})}' 
                }
            )
        )
        
        # Connect processors
        nipyapi.canvas.create_connection(source_processor, update_metadata, ['matched'])
        nipyapi.canvas.create_connection(update_metadata, cleanse_data, ['success'])
        nipyapi.canvas.create_connection(cleanse_data, jolt_transform, ['success'])
        nipyapi.canvas.create_connection(jolt_transform, evaluate_json, ['success'])
        
        logger.info("Transform flow created successfully")
        
        return {
            'update_metadata': update_metadata.id,
            'cleanse_data': cleanse_data.id,
            'jolt_transform': jolt_transform.id,
            'evaluate_json': evaluate_json.id
        }


def main():
    """Main transformation function for testing"""
    import yaml
    from extract import ProductExtractor
    
    logging.basicConfig(level=logging.INFO)
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Extract and transform
    extractor = ProductExtractor(config)
    transformer = ProductTransformer(config)
    
    records = extractor.extract()
    transformed_records = transformer.transform(records)
    
    for record in transformed_records:
        print(record)


if __name__ == '__main__':
    main()