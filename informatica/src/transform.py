"""
Transform module for m_LOAD_STG_SALES
Adds metadata fields: LOAD_DATE, SOURCE_SYSTEM, RECORD_ID
"""

import logging
import pandas as pd
import yaml
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class SalesDataTransformer:
    """Transform sales data by adding metadata fields"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize transformer with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.metadata_config = self.config['metadata']
        self.target_config = self.config['target']
        
        # Initialize sequence counter for RECORD_ID
        self.record_id_counter = self.metadata_config['record_id_start']
    
    def add_metadata_fields(self, df: pd.DataFrame, 
                           load_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Add metadata fields to the DataFrame
        - LOAD_DATE: Current timestamp
        - SOURCE_SYSTEM: Static value from config
        - RECORD_ID: Auto-incrementing sequence
        """
        logger.info(f"Adding metadata fields to {len(df)} records")
        
        if df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return df
        
        # Make a copy to avoid modifying original
        transformed_df = df.copy()
        
        # Add LOAD_DATE
        if load_date is None:
            load_date = datetime.now()
        
        load_date_field = self.metadata_config['load_date_field']
        transformed_df[load_date_field] = load_date
        logger.info(f"Added {load_date_field}: {load_date}")
        
        # Add SOURCE_SYSTEM
        source_system = self.metadata_config['source_system']
        transformed_df['SOURCE_SYSTEM'] = source_system
        logger.info(f"Added SOURCE_SYSTEM: {source_system}")
        
        # Add RECORD_ID (sequence)
        record_id_field = self.metadata_config['record_id_field']
        increment = self.metadata_config['record_id_increment']
        
        num_records = len(transformed_df)
        record_ids = range(
            self.record_id_counter,
            self.record_id_counter + (num_records * increment),
            increment
        )
        
        transformed_df[record_id_field] = list(record_ids)
        
        # Update counter for next batch
        self.record_id_counter += num_records * increment
        
        logger.info(f"Added {record_id_field}: {num_records} sequence values starting from {self.record_id_counter - (num_records * increment)}")
        
        return transformed_df
    
    def reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder columns to match target table structure"""
        target_fields = self.target_config['fields']
        
        # Only reorder columns that exist in the DataFrame
        existing_fields = [field for field in target_fields if field in df.columns]
        
        # Add any extra columns at the end
        extra_fields = [col for col in df.columns if col not in existing_fields]
        
        reordered_df = df[existing_fields + extra_fields]
        
        logger.info(f"Reordered columns to match target structure")
        
        return reordered_df
    
    def apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all transformations:
        1. Add metadata fields
        2. Reorder columns
        3. Final validation
        """
        logger.info(f"Starting transformation of {len(df)} records")
        
        # Add metadata
        transformed_df = self.add_metadata_fields(df)
        
        # Reorder columns
        transformed_df = self.reorder_columns(transformed_df)
        
        # Validate final output
        self._validate_output(transformed_df)
        
        logger.info(f"Transformation completed: {len(transformed_df)} records ready for load")
        
        return transformed_df
    
    def _validate_output(self, df: pd.DataFrame):
        """Validate transformed data before load"""
        target_fields = self.target_config['fields']
        
        # Check for missing required fields
        missing_fields = set(target_fields) - set(df.columns)
        if missing_fields:
            raise ValueError(f"Missing required target fields: {missing_fields}")
        
        # Check for null values in key fields
        key_fields = ['RECORD_ID', 'TRANSACTION_ID', 'LOAD_DATE', 'SOURCE_SYSTEM']
        for field in key_fields:
            if field in df.columns:
                null_count = df[field].isna().sum()
                if null_count > 0:
                    logger.warning(f"Found {null_count} null values in key field: {field}")
        
        # Log data quality metrics
        logger.info("Output validation summary:")
        logger.info(f"  Total records: {len(df)}")
        logger.info(f"  Total columns: {len(df.columns)}")
        logger.info(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    
    def reset_sequence(self, start_value: Optional[int] = None):
        """Reset the RECORD_ID sequence counter"""
        if start_value is None:
            start_value = self.metadata_config['record_id_start']
        
        self.record_id_counter = start_value
        logger.info(f"Reset RECORD_ID sequence to {start_value}")


if __name__ == "__main__":
    # Test transformation
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data
    test_data = pd.DataFrame({
        'TRANSACTION_ID': ['T001', 'T002', 'T003'],
        'TRANSACTION_DATE': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'CUSTOMER_ID': ['C001', 'C002', 'C003'],
        'PRODUCT_ID': ['P001', 'P002', 'P003'],
        'QUANTITY': [5, 10, 3],
        'UNIT_PRICE': [10.50, 25.00, 15.75],
        'DISCOUNT_PERCENT': [0, 5, 10],
        'TAX_AMOUNT': [5.25, 11.88, 4.25],
        'TOTAL_AMOUNT': [57.75, 249.38, 46.75],
        'PAYMENT_METHOD': ['CARD', 'CASH', 'CARD'],
        'STORE_ID': ['S001', 'S002', 'S001'],
        'REGION': ['EAST', 'WEST', 'EAST']
    })
    
    transformer = SalesDataTransformer()
    transformed = transformer.apply_transformations(test_data)
    
    print(f"\nTransformed data ({len(transformed)} rows):")
    print(transformed)
    print(f"\nColumns: {list(transformed.columns)}")