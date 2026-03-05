"""
Filter module for m_LOAD_STG_SALES
Implements quality filters: QUANTITY > 0 and TOTAL_AMOUNT > 0
"""

import logging
import pandas as pd
import yaml
from typing import Tuple

logger = logging.getLogger(__name__)


class SalesDataFilter:
    """Apply quality filters to sales data"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize filter with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.quality_filters = self.config['quality_filters']
        self.validation_config = self.config['validation']
        self.error_config = self.config['error_handling']
    
    def apply_quality_filters(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Apply quality filters based on configuration
        Returns: (valid_records, rejected_records)
        """
        logger.info(f"Applying quality filters to {len(df)} records")
        
        initial_count = len(df)
        rejected_records = pd.DataFrame()
        
        # Create rejection tracking
        df['_rejection_reason'] = ''
        df['_is_valid'] = True
        
        # Filter 1: QUANTITY > 0
        quantity_threshold = self.quality_filters['quantity_threshold']
        quantity_mask = (df['QUANTITY'].isna()) | (df['QUANTITY'] <= quantity_threshold)
        
        if quantity_mask.any():
            rejected_count = quantity_mask.sum()
            logger.warning(f"Rejecting {rejected_count} records with QUANTITY <= {quantity_threshold}")
            df.loc[quantity_mask, '_rejection_reason'] += 'QUANTITY_INVALID;'
            df.loc[quantity_mask, '_is_valid'] = False
        
        # Filter 2: TOTAL_AMOUNT > 0
        amount_threshold = self.quality_filters['total_amount_threshold']
        amount_mask = (df['TOTAL_AMOUNT'].isna()) | (df['TOTAL_AMOUNT'] <= amount_threshold)
        
        if amount_mask.any():
            rejected_count = amount_mask.sum()
            logger.warning(f"Rejecting {rejected_count} records with TOTAL_AMOUNT <= {amount_threshold}")
            df.loc[amount_mask, '_rejection_reason'] += 'TOTAL_AMOUNT_INVALID;'
            df.loc[amount_mask, '_is_valid'] = False
        
        # Validation: Check for null customer_id
        if not self.validation_config['allow_null_customer_id']:
            customer_null_mask = df['CUSTOMER_ID'].isna() | (df['CUSTOMER_ID'] == '')
            if customer_null_mask.any():
                rejected_count = customer_null_mask.sum()
                logger.warning(f"Rejecting {rejected_count} records with null CUSTOMER_ID")
                df.loc[customer_null_mask, '_rejection_reason'] += 'CUSTOMER_ID_NULL;'
                df.loc[customer_null_mask, '_is_valid'] = False
        
        # Validation: Check for null product_id
        if not self.validation_config['allow_null_product_id']:
            product_null_mask = df['PRODUCT_ID'].isna() | (df['PRODUCT_ID'] == '')
            if product_null_mask.any():
                rejected_count = product_null_mask.sum()
                logger.warning(f"Rejecting {rejected_count} records with null PRODUCT_ID")
                df.loc[product_null_mask, '_rejection_reason'] += 'PRODUCT_ID_NULL;'
                df.loc[product_null_mask, '_is_valid'] = False
        
        # Additional validation: Negative values
        if not self.validation_config['allow_negative_quantity']:
            negative_qty_mask = df['QUANTITY'] < 0
            if negative_qty_mask.any():
                rejected_count = negative_qty_mask.sum()
                logger.warning(f"Rejecting {rejected_count} records with negative QUANTITY")
                df.loc[negative_qty_mask, '_rejection_reason'] += 'QUANTITY_NEGATIVE;'
                df.loc[negative_qty_mask, '_is_valid'] = False
        
        if not self.validation_config['allow_negative_amount']:
            negative_amt_mask = df['TOTAL_AMOUNT'] < 0
            if negative_amt_mask.any():
                rejected_count = negative_amt_mask.sum()
                logger.warning(f"Rejecting {rejected_count} records with negative TOTAL_AMOUNT")
                df.loc[negative_amt_mask, '_rejection_reason'] += 'TOTAL_AMOUNT_NEGATIVE;'
                df.loc[negative_amt_mask, '_is_valid'] = False
        
        # Separate valid and rejected records
        valid_df = df[df['_is_valid']].copy()
        rejected_df = df[~df['_is_valid']].copy()
        
        # Clean up temporary columns from valid records
        valid_df = valid_df.drop(columns=['_rejection_reason', '_is_valid'])
        
        # Log results
        valid_count = len(valid_df)
        rejected_count = len(rejected_df)
        
        logger.info(f"Quality filter results:")
        logger.info(f"  Initial records: {initial_count}")
        logger.info(f"  Valid records: {valid_count}")
        logger.info(f"  Rejected records: {rejected_count}")
        logger.info(f"  Pass rate: {(valid_count/initial_count)*100:.2f}%")
        
        if rejected_count > 0:
            self._log_rejection_summary(rejected_df)
        
        return valid_df, rejected_df
    
    def _log_rejection_summary(self, rejected_df: pd.DataFrame):
        """Log summary of rejection reasons"""
        if rejected_df.empty:
            return
        
        logger.info("Rejection reasons summary:")
        
        # Count each rejection reason
        all_reasons = rejected_df['_rejection_reason'].str.split(';').explode()
        reason_counts = all_reasons[all_reasons != ''].value_counts()
        
        for reason, count in reason_counts.items():
            logger.info(f"  {reason}: {count} records")
    
    def save_rejected_records(self, rejected_df: pd.DataFrame, output_path: str = None):
        """Save rejected records to error log"""
        if rejected_df.empty:
            logger.info("No rejected records to save")
            return
        
        if output_path is None:
            output_path = self.error_config['error_log_path']
        
        try:
            # Add timestamp
            rejected_df['_error_timestamp'] = pd.Timestamp.now()
            
            # Append to error log
            rejected_df.to_csv(
                output_path,
                mode='a',
                header=not pd.io.common.file_exists(output_path),
                index=False
            )
            
            logger.info(f"Saved {len(rejected_df)} rejected records to {output_path}")
        
        except Exception as e:
            logger.error(f"Error saving rejected records: {str(e)}")
            raise


if __name__ == "__main__":
    # Test filtering
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data
    test_data = pd.DataFrame({
        'TRANSACTION_ID': ['T001', 'T002', 'T003', 'T004', 'T005'],
        'CUSTOMER_ID': ['C001', 'C002', None, 'C004', 'C005'],
        'PRODUCT_ID': ['P001', 'P002', 'P003', None, 'P005'],
        'QUANTITY': [5, 0, 10, 3, -1],
        'TOTAL_AMOUNT': [100.50, 50.00, 0, 75.25, 200.00]
    })
    
    filter_engine = SalesDataFilter()
    valid, rejected = filter_engine.apply_quality_filters(test_data)
    
    print(f"\nValid records: {len(valid)}")
    print(valid)
    print(f"\nRejected records: {len(rejected)}")
    print(rejected[['TRANSACTION_ID', '_rejection_reason']])