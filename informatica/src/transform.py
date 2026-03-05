"""
Transform module for Sales Fact ETL
Implements date key calculation, Customer SCD Type 2 temporal join,
Product dimension lookup with cost price retrieval, and business calculations
"""

import logging
from datetime import datetime
from typing import Dict, Optional, Tuple
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class SalesTransformer:
    """Transform sales data with dimension lookups and calculations"""
    
    def __init__(self, config: Dict):
        """
        Initialize transformer with configuration
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.engine: Optional[Engine] = None
        self.customer_dim_cache: Optional[pd.DataFrame] = None
        self.product_dim_cache: Optional[pd.DataFrame] = None
        
    def connect(self) -> None:
        """Establish database connection"""
        try:
            db_config = self.config['database']
            connection_string = (
                f"{db_config['type']}://{db_config['username']}:{db_config['password']}"
                f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )
            self.engine = create_engine(connection_string, pool_pre_ping=True)
            logger.info("Database connection established for transformations")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def load_dimension_caches(self) -> None:
        """Load dimension tables into memory for lookups"""
        if not self.engine:
            self.connect()
        
        transform_config = self.config['transform']
        
        try:
            # Load Customer Dimension (SCD Type 2 - active records only for initial cache)
            customer_query = f"""
            SELECT 
                CUSTOMER_KEY,
                CUSTOMER_ID,
                EFFECTIVE_FROM_DATE,
                EFFECTIVE_TO_DATE,
                IS_CURRENT
            FROM {transform_config['customer_dimension_table']}
            ORDER BY CUSTOMER_ID, EFFECTIVE_FROM_DATE
            """
            self.customer_dim_cache = pd.read_sql(customer_query, self.engine)
            self.customer_dim_cache['EFFECTIVE_FROM_DATE'] = pd.to_datetime(
                self.customer_dim_cache['EFFECTIVE_FROM_DATE']
            )
            self.customer_dim_cache['EFFECTIVE_TO_DATE'] = pd.to_datetime(
                self.customer_dim_cache['EFFECTIVE_TO_DATE']
            )
            logger.info(f"Loaded {len(self.customer_dim_cache)} customer dimension records")
            
            # Load Product Dimension (SCD Type 1 - current records only)
            product_query = f"""
            SELECT 
                PRODUCT_KEY,
                PRODUCT_ID,
                COST_PRICE,
                UNIT_PRICE,
                PRODUCT_NAME,
                CATEGORY
            FROM {transform_config['product_dimension_table']}
            """
            self.product_dim_cache = pd.read_sql(product_query, self.engine)
            logger.info(f"Loaded {len(self.product_dim_cache)} product dimension records")
            
        except Exception as e:
            logger.error(f"Failed to load dimension caches: {str(e)}")
            raise
    
    def calculate_date_key(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate date key from transaction date (format: YYYYMMDD)
        
        Args:
            df: DataFrame with TRANSACTION_DATE column
            
        Returns:
            DataFrame with DATE_KEY column added
        """
        logger.info("Calculating date keys")
        
        # Ensure TRANSACTION_DATE is datetime
        df['TRANSACTION_DATE'] = pd.to_datetime(df['TRANSACTION_DATE'])
        
        # Calculate date key as integer YYYYMMDD
        df['DATE_KEY'] = df['TRANSACTION_DATE'].dt.strftime('%Y%m%d').astype(int)
        
        # Also keep date-only version for lookups
        df['TRANSACTION_DATE_ONLY'] = df['TRANSACTION_DATE'].dt.date
        df['TRANSACTION_DATE_ONLY'] = pd.to_datetime(df['TRANSACTION_DATE_ONLY'])
        
        logger.info(f"Date keys calculated for {len(df)} records")
        return df
    
    def lookup_customer_key_temporal(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform SCD Type 2 temporal join to get customer key
        Match on CUSTOMER_ID and transaction date within effective date range
        
        Args:
            df: DataFrame with CUSTOMER_ID and TRANSACTION_DATE_ONLY
            
        Returns:
            DataFrame with CUSTOMER_KEY column added
        """
        logger.info("Performing SCD Type 2 temporal customer lookup")
        
        if self.customer_dim_cache is None:
            self.load_dimension_caches()
        
        # Prepare for merge
        df_with_keys = df.copy()
        
        # Perform temporal join using pandas merge_asof or iterative matching
        # For each transaction, find the customer record where:
        # - CUSTOMER_ID matches
        # - TRANSACTION_DATE is between EFFECTIVE_FROM_DATE and EFFECTIVE_TO_DATE
        
        customer_keys = []
        unmatched_count = 0
        
        for idx, row in df.iterrows():
            customer_id = row['CUSTOMER_ID']
            trans_date = row['TRANSACTION_DATE_ONLY']
            
            # Filter customer dimension for matching customer_id
            matching_customers = self.customer_dim_cache[
                self.customer_dim_cache['CUSTOMER_ID'] == customer_id
            ]
            
            if len(matching_customers) == 0:
                customer_keys.append(None)
                unmatched_count += 1
                continue
            
            # Find the record where transaction date falls within effective range
            temporal_match = matching_customers[
                (matching_customers['EFFECTIVE_FROM_DATE'] <= trans_date) &
                (matching_customers['EFFECTIVE_TO_DATE'] >= trans_date)
            ]
            
            if len(temporal_match) > 0:
                # Take the first match (should be only one)
                customer_keys.append(temporal_match.iloc[0]['CUSTOMER_KEY'])
            else:
                customer_keys.append(None)
                unmatched_count += 1
        
        df_with_keys['CUSTOMER_KEY'] = customer_keys
        
        if unmatched_count > 0:
            logger.warning(f"Could not find customer key for {unmatched_count} records")
        else:
            logger.info("All customer keys resolved successfully")
        
        return df_with_keys
    
    def lookup_product_key_and_cost(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Lookup product key and cost price from product dimension
        
        Args:
            df: DataFrame with PRODUCT_ID
            
        Returns:
            DataFrame with PRODUCT_KEY and COST_PRICE columns added
        """
        logger.info("Performing product dimension lookup")
        
        if self.product_dim_cache is None:
            self.load_dimension_caches()
        
        # Merge with product dimension
        df_with_product = df.merge(
            self.product_dim_cache[['PRODUCT_ID', 'PRODUCT_KEY', 'COST_PRICE']],
            on='PRODUCT_ID',
            how='left'
        )
        
        # Check for unmatched products
        unmatched_count = df_with_product['PRODUCT_KEY'].isnull().sum()
        if unmatched_count > 0:
            logger.warning(f"Could not find product key for {unmatched_count} records")
        else:
            logger.info("All product keys resolved successfully")
        
        return df_with_product
    
    def calculate_business_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate derived business metrics
        
        Args:
            df: DataFrame with transaction details
            
        Returns:
            DataFrame with calculated metrics
        """
        logger.info("Calculating business metrics")
        
        # Calculate discount amount
        df['DISCOUNT_AMOUNT'] = (
            df['UNIT_PRICE'] * df['QUANTITY'] * df['DISCOUNT_PERCENT'] / 100
        ).round(2)
        
        # Calculate cost amount using lookup cost price
        df['COST_AMOUNT'] = (df['COST_PRICE'] * df['QUANTITY']).round(2)
        
        # Calculate profit amount
        # Profit = Total Amount - Tax - Cost
        df['PROFIT_AMOUNT'] = (
            df['TOTAL_AMOUNT'] - df['TAX_AMOUNT'] - df['COST_AMOUNT']
        ).round(2)
        
        # Calculate profit margin percentage
        # Profit Margin % = (Profit / (Total - Tax)) * 100
        df['NET_SALES'] = df['TOTAL_AMOUNT'] - df['TAX_AMOUNT']
        df['PROFIT_MARGIN_PERCENT'] = np.where(
            df['NET_SALES'] > 0,
            ((df['PROFIT_AMOUNT'] / df['NET_SALES']) * 100).round(2),
            0
        )
        
        # Add load timestamp
        df['LOAD_TIMESTAMP'] = datetime.now()
        
        logger.info("Business metrics calculated")
        return df
    
    def filter_valid_records(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Filter valid records and separate invalid ones for error logging
        
        Args:
            df: DataFrame to filter
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        logger.info("Filtering valid records")
        
        # Define validity conditions
        valid_mask = (
            df['CUSTOMER_KEY'].notna() &
            df['PRODUCT_KEY'].notna() &
            (df['QUANTITY'] > 0) &
            (df['TOTAL_AMOUNT'] > 0) &
            (df['COST_AMOUNT'] >= 0)
        )
        
        valid_df = df[valid_mask].copy()
        invalid_df = df[~valid_mask].copy()
        
        logger.info(f"Valid records: {len(valid_df)}, Invalid records: {len(invalid_df)}")
        
        if len(invalid_df) > 0:
            # Add error reasons
            invalid_df['ERROR_REASON'] = ''
            invalid_df.loc[invalid_df['CUSTOMER_KEY'].isna(), 'ERROR_REASON'] += 'Missing Customer Key; '
            invalid_df.loc[invalid_df['PRODUCT_KEY'].isna(), 'ERROR_REASON'] += 'Missing Product Key; '
            invalid_df.loc[invalid_df['QUANTITY'] <= 0, 'ERROR_REASON'] += 'Invalid Quantity; '
            invalid_df.loc[invalid_df['TOTAL_AMOUNT'] <= 0, 'ERROR_REASON'] += 'Invalid Total Amount; '
            
            logger.warning(f"Invalid records summary:\n{invalid_df['ERROR_REASON'].value_counts()}")
        
        return valid_df, invalid_df
    
    def transform(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Main transformation pipeline
        
        Args:
            df: Raw extracted DataFrame
            
        Returns:
            Tuple of (transformed_df, error_df)
        """
        logger.info("Starting transformation pipeline")
        
        # Load dimension caches if not already loaded
        if self.customer_dim_cache is None or self.product_dim_cache is None:
            self.load_dimension_caches()
        
        # Step 1: Calculate date key
        df = self.calculate_date_key(df)
        
        # Step 2: Lookup customer key with SCD Type 2 temporal join
        df = self.lookup_customer_key_temporal(df)
        
        # Step 3: Lookup product key and cost price
        df = self.lookup_product_key_and_cost(df)
        
        # Step 4: Calculate business metrics
        df = self.calculate_business_metrics(df)
        
        # Step 5: Filter valid records
        valid_df, invalid_df = self.filter_valid_records(df)
        
        logger.info("Transformation pipeline completed")
        
        return valid_df, invalid_df
    
    def close(self) -> None:
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


def main():
    """Main execution function for testing"""
    import yaml
    from extract import SalesExtractor
    
    # Load configuration
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Extract data
    extractor = SalesExtractor(config)
    df = extractor.extract_sales_transactions()
    extractor.close()
    
    # Transform data
    transformer = SalesTransformer(config)
    
    try:
        valid_df, invalid_df = transformer.transform(df)
        
        print(f"\nTransformation Results:")
        print(f"Valid records: {len(valid_df)}")
        print(f"Invalid records: {len(invalid_df)}")
        
        print("\nValid records sample:")
        print(valid_df.head())
        
        if len(invalid_df) > 0:
            print("\nInvalid records sample:")
            print(invalid_df[['TRANSACTION_ID', 'ERROR_REASON']].head())
        
    finally:
        transformer.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    main()