"""
Extract module for m_LOAD_STG_SALES
Handles CSV file extraction with pattern matching and schema validation
"""

import os
import glob
import logging
from typing import List, Dict, Any, Generator
import pandas as pd
from datetime import datetime
import yaml

logger = logging.getLogger(__name__)


class SalesDataExtractor:
    """Extract sales transaction data from CSV files"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize extractor with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.source_config = self.config['source']
        self.validation_config = self.config['validation']
        
    def get_source_files(self) -> List[str]:
        """Get list of source files matching the pattern"""
        file_path = self.source_config['file_path']
        
        # Handle glob pattern
        if '*' in file_path:
            files = glob.glob(file_path)
            logger.info(f"Found {len(files)} files matching pattern: {file_path}")
            return sorted(files)
        else:
            if os.path.exists(file_path):
                logger.info(f"Found single file: {file_path}")
                return [file_path]
            else:
                raise FileNotFoundError(f"Source file not found: {file_path}")
    
    def validate_schema(self, df: pd.DataFrame) -> bool:
        """Validate DataFrame schema against configuration"""
        expected_fields = [field['name'] for field in self.source_config['fields']]
        actual_fields = df.columns.tolist()
        
        missing_fields = set(expected_fields) - set(actual_fields)
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        extra_fields = set(actual_fields) - set(expected_fields)
        if extra_fields and self.validation_config['enforce_schema']:
            logger.warning(f"Extra fields found (will be ignored): {extra_fields}")
        
        return True
    
    def parse_datatypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse and convert datatypes based on configuration"""
        for field in self.source_config['fields']:
            field_name = field['name']
            field_type = field['type']
            
            if field_name not in df.columns:
                continue
            
            try:
                if field_type == 'integer':
                    df[field_name] = pd.to_numeric(df[field_name], errors='coerce').astype('Int64')
                
                elif field_type == 'decimal':
                    df[field_name] = pd.to_numeric(df[field_name], errors='coerce').round(field.get('scale', 2))
                
                elif field_type == 'datetime':
                    date_format = field.get('format', '%Y-%m-%d %H:%M:%S')
                    df[field_name] = pd.to_datetime(df[field_name], format=date_format, errors='coerce')
                
                elif field_type == 'string':
                    df[field_name] = df[field_name].astype(str).str.strip()
                    max_length = field.get('length')
                    if max_length:
                        df[field_name] = df[field_name].str[:max_length]
            
            except Exception as e:
                logger.error(f"Error parsing field {field_name}: {str(e)}")
                raise
        
        return df
    
    def extract_from_file(self, file_path: str) -> pd.DataFrame:
        """Extract data from a single CSV file"""
        logger.info(f"Extracting data from: {file_path}")
        
        try:
            df = pd.read_csv(
                file_path,
                delimiter=self.source_config['delimiter'],
                encoding=self.source_config['encoding'],
                header=0 if self.source_config['has_header'] else None
            )
            
            logger.info(f"Extracted {len(df)} rows from {file_path}")
            
            # Validate schema
            self.validate_schema(df)
            
            # Parse datatypes
            df = self.parse_datatypes(df)
            
            # Add source file metadata
            df['_source_file'] = os.path.basename(file_path)
            df['_extract_timestamp'] = datetime.now()
            
            return df
        
        except Exception as e:
            logger.error(f"Error extracting from {file_path}: {str(e)}")
            raise
    
    def extract_all(self) -> Generator[pd.DataFrame, None, None]:
        """Extract data from all source files"""
        files = self.get_source_files()
        
        if not files:
            logger.warning("No source files found")
            return
        
        for file_path in files:
            try:
                df = self.extract_from_file(file_path)
                yield df
            except Exception as e:
                logger.error(f"Failed to extract from {file_path}: {str(e)}")
                if self.config['error_handling']['max_retry_attempts'] == 0:
                    raise
                continue
    
    def extract_batch(self, batch_size: int = None) -> pd.DataFrame:
        """Extract all data and return as single DataFrame"""
        if batch_size is None:
            batch_size = self.config['performance']['batch_size']
        
        all_data = []
        for df in self.extract_all():
            all_data.append(df)
        
        if not all_data:
            return pd.DataFrame()
        
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total extracted rows: {len(combined_df)}")
        
        return combined_df


if __name__ == "__main__":
    # Test extraction
    logging.basicConfig(level=logging.INFO)
    
    extractor = SalesDataExtractor()
    df = extractor.extract_batch()
    
    print(f"\nExtracted {len(df)} total rows")
    print(f"\nSample data:\n{df.head()}")
    print(f"\nData types:\n{df.dtypes}")