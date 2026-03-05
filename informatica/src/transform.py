"""
Transform module for product data validation and edge case handling.
Implements comprehensive validation rules and data quality checks.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import numpy as np
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class ProductDataTransformer:
    """Transform and validate product data with comprehensive edge case handling."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transformer with validation rules.
        
        Args:
            config: Configuration dictionary with validation rules
        """
        self.config = config
        self.validation_rules = config.get('validation_rules', {})
        self.errors = []
        self.warnings = []
        
    def transform(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Transform and validate product data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (valid_records_df, invalid_records_df)
        """
        logger.info(f"Starting transformation of {len(df)} records")
        
        # Create a copy to avoid modifying original
        df_work = df.copy()
        
        # Add validation columns
        df_work['IS_VALID'] = True
        df_work['VALIDATION_ERRORS'] = ''
        df_work['VALIDATION_WARNINGS'] = ''
        
        # Execute transformation steps
        df_work = self._clean_text_fields(df_work)
        df_work = self._validate_required_fields(df_work)
        df_work = self._validate_and_fix_prices(df_work)
        df_work = self._validate_price_consistency(df_work)
        df_work = self._handle_null_values(df_work)
        df_work = self._validate_data_quality(df_work)
        df_work = self._calculate_derived_fields(df_work)
        df_work = self._add_metadata(df_work)
        
        # Split into valid and invalid records
        valid_df = df_work[df_work['IS_VALID'] == True].copy()
        invalid_df = df_work[df_work['IS_VALID'] == False].copy()
        
        logger.info(f"Transformation complete: {len(valid_df)} valid, {len(invalid_df)} invalid")
        
        return valid_df, invalid_df
    
    def _clean_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize text fields."""
        logger.info("Cleaning text fields")
        
        text_fields = [
            'PRODUCT_NAME', 'PRODUCT_DESCRIPTION', 'CATEGORY',
            'SUB_CATEGORY', 'BRAND', 'SUPPLIER_NAME', 'STATUS'
        ]
        
        for field in text_fields:
            if field in df.columns:
                # Trim whitespace
                df[field] = df[field].astype(str).str.strip()
                
                # Handle case based on field type
                if field in ['CATEGORY', 'SUB_CATEGORY', 'STATUS']:
                    df[f'{field}_CLEAN'] = df[field].str.upper()
                elif field in ['PRODUCT_NAME', 'BRAND', 'SUPPLIER_NAME']:
                    df[f'{field}_CLEAN'] = df[field].str.title()
                else:
                    df[f'{field}_CLEAN'] = df[field]
                
                # Replace empty strings with None
                df[f'{field}_CLEAN'] = df[f'{field}_CLEAN'].replace(['', 'NONE', 'NULL', 'NAN'], np.nan)
        
        return df
    
    def _validate_required_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate required fields are present and not null."""
        logger.info("Validating required fields")
        
        required_fields = self.validation_rules.get('required_fields', [
            'PRODUCT_ID', 'PRODUCT_NAME_CLEAN', 'CATEGORY_CLEAN'
        ])
        
        for field in required_fields:
            if field not in df.columns:
                logger.error(f"Required field {field} not found in DataFrame")
                continue
            
            null_mask = df[field].isnull()
            if null_mask.any():
                error_msg = f"Missing required field: {field}"
                df.loc[null_mask, 'VALIDATION_ERRORS'] += error_msg + '; '
                df.loc[null_mask, 'IS_VALID'] = False
                logger.warning(f"{null_mask.sum()} records missing {field}")
        
        return df
    
    def _validate_and_fix_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and fix price fields including zero and negative price handling.
        """
        logger.info("Validating and fixing price fields")
        
        price_fields = ['UNIT_PRICE', 'COST_PRICE']
        
        for field in price_fields:
            if field not in df.columns:
                continue
            
            # Convert to numeric, handling errors
            df[field] = pd.to_numeric(df[field], errors='coerce')
            
            # Handle null prices
            null_mask = df[field].isnull()
            if null_mask.any():
                default_value = self.validation_rules.get(f'default_{field.lower()}', 0.0)
                df.loc[null_mask, field] = default_value
                df.loc[null_mask, 'VALIDATION_WARNINGS'] += f"{field} was null, set to default {default_value}; "
                logger.info(f"Set {null_mask.sum()} null {field} values to {default_value}")
            
            # Handle negative prices
            negative_mask = df[field] < 0
            if negative_mask.any():
                action = self.validation_rules.get('negative_price_action', 'reject')
                
                if action == 'reject':
                    df.loc[negative_mask, 'VALIDATION_ERRORS'] += f"{field} is negative; "
                    df.loc[negative_mask, 'IS_VALID'] = False
                    logger.warning(f"Rejected {negative_mask.sum()} records with negative {field}")
                elif action == 'absolute':
                    df.loc[negative_mask, field] = df.loc[negative_mask, field].abs()
                    df.loc[negative_mask, 'VALIDATION_WARNINGS'] += f"{field} was negative, converted to absolute; "
                    logger.info(f"Converted {negative_mask.sum()} negative {field} to absolute")
                elif action == 'zero':
                    df.loc[negative_mask, field] = 0.0
                    df.loc[negative_mask, 'VALIDATION_WARNINGS'] += f"{field} was negative, set to 0; "
                    logger.info(f"Set {negative_mask.sum()} negative {field} to zero")
            
            # Handle zero prices
            zero_mask = df[field] == 0
            if zero_mask.any():
                action = self.validation_rules.get('zero_price_action', 'warn')
                
                if action == 'reject':
                    df.loc[zero_mask, 'VALIDATION_ERRORS'] += f"{field} is zero; "
                    df.loc[zero_mask, 'IS_VALID'] = False
                    logger.warning(f"Rejected {zero_mask.sum()} records with zero {field}")
                elif action == 'warn':
                    df.loc[zero_mask, 'VALIDATION_WARNINGS'] += f"{field} is zero; "
                    logger.info(f"Warning for {zero_mask.sum()} records with zero {field}")
            
            # Round prices to 2 decimal places
            df[f'{field}_CLEAN'] = df[field].round(2)
        
        return df
    
    def _validate_price_consistency(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate price consistency between unit price and cost price.
        """
        logger.info("Validating price consistency")
        
        # Check if cost price exceeds unit price
        if 'UNIT_PRICE_CLEAN' in df.columns and 'COST_PRICE_CLEAN' in df.columns:
            inconsistent_mask = df['COST_PRICE_CLEAN'] > df['UNIT_PRICE_CLEAN']
            
            if inconsistent_mask.any():
                action = self.validation_rules.get('price_inconsistency_action', 'warn')
                
                if action == 'reject':
                    df.loc[inconsistent_mask, 'VALIDATION_ERRORS'] += "Cost price exceeds unit price; "
                    df.loc[inconsistent_mask, 'IS_VALID'] = False
                    logger.warning(f"Rejected {inconsistent_mask.sum()} records with cost > unit price")
                elif action == 'warn':
                    df.loc[inconsistent_mask, 'VALIDATION_WARNINGS'] += "Cost price exceeds unit price; "
                    logger.info(f"Warning for {inconsistent_mask.sum()} records with cost > unit price")
                elif action == 'swap':
                    # Swap the prices
                    temp = df.loc[inconsistent_mask, 'UNIT_PRICE_CLEAN'].copy()
                    df.loc[inconsistent_mask, 'UNIT_PRICE_CLEAN'] = df.loc[inconsistent_mask, 'COST_PRICE_CLEAN']
                    df.loc[inconsistent_mask, 'COST_PRICE_CLEAN'] = temp
                    df.loc[inconsistent_mask, 'VALIDATION_WARNINGS'] += "Swapped cost and unit prices; "
                    logger.info(f"Swapped prices for {inconsistent_mask.sum()} records")
            
            # Check for unrealistic profit margins
            min_margin = self.validation_rules.get('min_profit_margin_percent', -50)
            max_margin = self.validation_rules.get('max_profit_margin_percent', 500)
            
            df['TEMP_MARGIN'] = np.where(
                df['UNIT_PRICE_CLEAN'] > 0,
                ((df['UNIT_PRICE_CLEAN'] - df['COST_PRICE_CLEAN']) / df['UNIT_PRICE_CLEAN'] * 100),
                0
            )
            
            unrealistic_margin_mask = (
                (df['TEMP_MARGIN'] < min_margin) | 
                (df['TEMP_MARGIN'] > max_margin)
            )
            
            if unrealistic_margin_mask.any():
                df.loc[unrealistic_margin_mask, 'VALIDATION_WARNINGS'] += \
                    f"Profit margin outside range [{min_margin}%, {max_margin}%]; "
                logger.info(f"Warning for {unrealistic_margin_mask.sum()} records with unusual margins")
            
            df.drop('TEMP_MARGIN', axis=1, inplace=True)
        
        return df
    
    def _handle_null_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle null values in optional fields with appropriate defaults.
        """
        logger.info("Handling null values in optional fields")
        
        null_handling_rules = self.validation_rules.get('null_handling', {})
        
        # Handle numeric fields
        numeric_defaults = null_handling_rules.get('numeric_defaults', {
            'WEIGHT': 0.0,
            'SUPPLIER_ID': 'UNKNOWN'
        })
        
        for field, default_value in numeric_defaults.items():
            if field in df.columns:
                null_mask = df[field].isnull()
                if null_mask.any():
                    df.loc[null_mask, field] = default_value
                    df.loc[null_mask, 'VALIDATION_WARNINGS'] += f"{field} was null, set to {default_value}; "
                    logger.info(f"Set {null_mask.sum()} null {field} values to {default_value}")
        
        # Handle text fields
        text_defaults = null_handling_rules.get('text_defaults', {
            'COLOR': 'N/A',
            'SIZE': 'N/A',
            'MATERIAL': 'N/A',
            'DIMENSIONS': 'N/A'
        })
        
        for field, default_value in text_defaults.items():
            if field in df.columns:
                df[field] = df[field].fillna(default_value)
        
        # Handle description separately - can be empty but log it
        if 'PRODUCT_DESCRIPTION_CLEAN' in df.columns:
            null_desc_mask = df['PRODUCT_DESCRIPTION_CLEAN'].isnull()
            if null_desc_mask.any():
                df.loc[null_desc_mask, 'PRODUCT_DESCRIPTION_CLEAN'] = ''
                df.loc[null_desc_mask, 'VALIDATION_WARNINGS'] += "Product description is empty; "
                logger.info(f"{null_desc_mask.sum()} records have no product description")
        
        return df
    
    def _validate_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform additional data quality validations.
        """
        logger.info("Performing data quality validations")
        
        # Validate product ID format
        if 'PRODUCT_ID' in df.columns:
            id_pattern = self.validation_rules.get('product_id_pattern', r'^[A-Z0-9\-]+$')
            invalid_id_mask = ~df['PRODUCT_ID'].astype(str).str.match(id_pattern, na=False)
            
            if invalid_id_mask.any():
                df.loc[invalid_id_mask, 'VALIDATION_WARNINGS'] += "Invalid product ID format; "
                logger.info(f"{invalid_id_mask.sum()} records have invalid product ID format")
        
        # Validate status values
        if 'STATUS_CLEAN' in df.columns:
            valid_statuses = self.validation_rules.get('valid_statuses', [
                'ACTIVE', 'INACTIVE', 'DISCONTINUED', 'OUT_OF_STOCK'
            ])
            invalid_status_mask = ~df['STATUS_CLEAN'].isin(valid_statuses)
            
            if invalid_status_mask.any():
                df.loc[invalid_status_mask, 'VALIDATION_WARNINGS'] += \
                    f"Invalid status value, expected one of {valid_statuses}; "
                # Set to default
                df.loc[invalid_status_mask, 'STATUS_CLEAN'] = 'ACTIVE'
                logger.info(f"Set {invalid_status_mask.sum()} invalid status values to ACTIVE")
        
        # Validate category values
        if 'CATEGORY_CLEAN' in df.columns:
            valid_categories = self.validation_rules.get('valid_categories', [])
            if valid_categories:
                invalid_cat_mask = ~df['CATEGORY_CLEAN'].isin(valid_categories)
                
                if invalid_cat_mask.any():
                    action = self.validation_rules.get('invalid_category_action', 'warn')
                    
                    if action == 'reject':
                        df.loc[invalid_cat_mask, 'VALIDATION_ERRORS'] += "Invalid category; "
                        df.loc[invalid_cat_mask, 'IS_VALID'] = False
                        logger.warning(f"Rejected {invalid_cat_mask.sum()} records with invalid category")
                    else:
                        df.loc[invalid_cat_mask, 'VALIDATION_WARNINGS'] += "Invalid category; "
                        logger.info(f"{invalid_cat_mask.sum()} records have invalid category")
        
        return df
    
    def _calculate_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate derived fields like profit margin and price range.
        """
        logger.info("Calculating derived fields")
        
        # Calculate profit margin
        if 'UNIT_PRICE_CLEAN' in df.columns and 'COST_PRICE_CLEAN' in df.columns:
            df['PROFIT_MARGIN'] = np.where(
                df['UNIT_PRICE_CLEAN'] > 0,
                np.round(
                    ((df['UNIT_PRICE_CLEAN'] - df['COST_PRICE_CLEAN']) / 
                     df['UNIT_PRICE_CLEAN'] * 100),
                    2
                ),
                0.0
            )
            
            # Ensure profit margin is within reasonable bounds
            df['PROFIT_MARGIN'] = df['PROFIT_MARGIN'].clip(-100, 1000)
        
        # Calculate price range category
        if 'UNIT_PRICE_CLEAN' in df.columns:
            price_ranges = self.validation_rules.get('price_ranges', {
                'LOW': (0, 50),
                'MEDIUM': (50, 200),
                'HIGH': (200, 500),
                'PREMIUM': (500, float('inf'))
            })
            
            conditions = [
                df['UNIT_PRICE_CLEAN'].between(low, high, inclusive='left')
                for low, high in price_ranges.values()
            ]
            choices = list(price_ranges.keys())
            
            df['PRICE_RANGE'] = np.select(conditions, choices, default='UNKNOWN')
        
        # Calculate full product name with brand
        if 'BRAND_CLEAN' in df.columns and 'PRODUCT_NAME_CLEAN' in df.columns:
            df['FULL_PRODUCT_NAME'] = df['BRAND_CLEAN'] + ' - ' + df['PRODUCT_NAME_CLEAN']
        
        return df
    
    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add metadata fields."""
        logger.info("Adding metadata fields")
        
        df['TRANSFORM_TIMESTAMP'] = datetime.now()
        df['DATA_QUALITY_SCORE'] = self._calculate_quality_score(df)
        
        return df
    
    def _calculate_quality_score(self, df: pd.DataFrame) -> pd.Series:
        """
        Calculate data quality score for each record.
        
        Returns:
            Series with quality scores (0-100)
        """
        score = pd.Series(100, index=df.index)
        
        # Deduct points for warnings
        warning_count = df['VALIDATION_WARNINGS'].str.count(';')
        score -= warning_count * 5
        
        # Deduct points for missing optional fields
        optional_fields = ['PRODUCT_DESCRIPTION_CLEAN', 'BRAND_CLEAN', 'SUPPLIER_NAME']
        for field in optional_fields:
            if field in df.columns:
                score -= df[field].isnull().astype(int) * 3
        
        # Ensure score is between 0 and 100
        score = score.clip(0, 100)
        
        return score
    
    def get_transformation_summary(
        self,
        valid_df: pd.DataFrame,
        invalid_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Generate transformation summary statistics.
        
        Args:
            valid_df: Valid records DataFrame
            invalid_df: Invalid records DataFrame
            
        Returns:
            Dictionary of summary statistics
        """
        total_records = len(valid_df) + len(invalid_df)
        
        summary = {
            'total_records': total_records,
            'valid_records': len(valid_df),
            'invalid_records': len(invalid_df),
            'validation_rate': round(len(valid_df) / total_records * 100, 2) if total_records > 0 else 0,
            'records_with_warnings': len(valid_df[valid_df['VALIDATION_WARNINGS'] != '']),
            'average_quality_score': round(valid_df['DATA_QUALITY_SCORE'].mean(), 2) if len(valid_df) > 0 else 0,
            'timestamp': datetime.now().isoformat()
        }
        
        # Price statistics for valid records
        if len(valid_df) > 0 and 'UNIT_PRICE_CLEAN' in valid_df.columns:
            summary['price_statistics'] = {
                'avg_unit_price': round(valid_df['UNIT_PRICE_CLEAN'].mean(), 2),
                'min_unit_price': round(valid_df['UNIT_PRICE_CLEAN'].min(), 2),
                'max_unit_price': round(valid_df['UNIT_PRICE_CLEAN'].max(), 2),
                'zero_price_count': len(valid_df[valid_df['UNIT_PRICE_CLEAN'] == 0])
            }
            
            if 'PROFIT_MARGIN' in valid_df.columns:
                summary['price_statistics']['avg_profit_margin'] = round(
                    valid_df['PROFIT_MARGIN'].mean(), 2
                )
        
        return summary