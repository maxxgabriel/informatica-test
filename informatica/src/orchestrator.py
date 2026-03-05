"""
Orchestrator module for Customer Dimension Load pipeline
Coordinates extract, transform, and load with SCD Type 2 logic
"""
import logging
from typing import Dict, Any, List
from datetime import datetime

from src.extract import create_extractor
from src.transform import create_transformer
from src.load import create_loader

logger = logging.getLogger(__name__)


class CustomerDimensionOrchestrator:
    """Orchestrates the complete customer dimension load pipeline"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.extractor = create_extractor(config)
        self.transformer = create_transformer(config)
        self.loader = create_loader(config)
        
    def analyze_scd_for_batch(
        self, 
        cleansed_records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Analyze each record for SCD Type 2 logic by looking up existing records
        
        Args:
            cleansed_records: List of cleansed customer records
            
        Returns:
            List of analysis dicts with existing_record, is_new, is_changed
        """
        conn = None
        analysis_results = []
        
        try:
            conn = self.loader.get_connection()
            cursor = conn.cursor()
            
            for cleansed in cleansed_records:
                customer_id = cleansed['CUSTOMER_ID']
                
                # Lookup existing record
                existing = self.loader.lookup_existing_customer(cursor, customer_id)
                
                # Detect changes
                is_new, is_changed = self.transformer.detect_changes(
                    cleansed, existing
                )
                
                analysis_results.append({
                    'existing_record': existing,
                    'is_new': is_new,
                    'is_changed': is_changed
                })
                
        except Exception as e:
            logger.error(f"Error analyzing SCD for batch: {e}")
            raise
        finally:
            if conn:
                conn.close()
                
        return analysis_results
    
    def run_pipeline(self, last_extract_date: datetime = None) -> Dict[str, Any]:
        """
        Execute complete customer dimension load pipeline
        
        Args:
            last_extract_date: Date to extract from (defaults to yesterday)
            
        Returns:
            Dictionary with pipeline execution statistics
        """
        logger.info("="*60)
        logger.info("Starting Customer Dimension Load Pipeline")
        logger.info("="*60)
        
        pipeline_stats = {
            'start_time': datetime.now(),
            'extracted_count': 0,
            'valid_count': 0,
            'transformed_count': 0,
            'load_stats': {},
            'errors': []
        }
        
        try:
            # Step 1: Extract from staging
            logger.info("Step 1: Extracting customer records from staging")
            valid_records = self.extractor.extract_and_validate(last_extract_date)
            pipeline_stats['extracted_count'] = len(valid_records)
            pipeline_stats['valid_count'] = len(valid_records)
            
            if not valid_records:
                logger.warning("No records to process")
                return pipeline_stats
            
            # Step 2: Transform and cleanse
            logger.info("Step 2: Transforming and cleansing customer records")
            cleansed_records = self.transformer.transform_batch(valid_records)
            pipeline_stats['transformed_count'] = len(cleansed_records)
            
            # Step 3: Analyze for SCD Type 2
            logger.info("Step 3: Analyzing records for SCD Type 2 logic")
            scd_analysis = self.analyze_scd_for_batch(cleansed_records)
            
            # Step 4: Load to dimension
            logger.info("Step 4: Loading to customer dimension with SCD Type 2")
            load_stats = self.loader.load_dimension_batch(
                cleansed_records, scd_analysis
            )
            pipeline_stats['load_stats'] = load_stats
            
            # Complete
            pipeline_stats['end_time'] = datetime.now()
            pipeline_stats['duration_seconds'] = (
                pipeline_stats['end_time'] - pipeline_stats['start_time']
            ).total_seconds()
            
            logger.info("="*60)
            logger.info("Customer Dimension Load Pipeline Complete")
            logger.info(f"Duration: {pipeline_stats['duration_seconds']:.2f} seconds")
            logger.info(f"Extracted: {pipeline_stats['extracted_count']}")
            logger.info(f"New records: {load_stats['new_records']}")
            logger.info(f"Changed records: {load_stats['changed_records']}")
            logger.info(f"Unchanged: {load_stats['unchanged_records']}")
            logger.info(f"Errors: {load_stats['errors']}")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            pipeline_stats['errors'].append(str(e))
            raise
            
        return pipeline_stats


def create_orchestrator(config: Dict[str, Any]) -> CustomerDimensionOrchestrator:
    """Factory function to create orchestrator instance"""
    return CustomerDimensionOrchestrator(config)