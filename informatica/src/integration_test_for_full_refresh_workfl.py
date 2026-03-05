===FILE: src/full_refresh_workflow.py===
"""
Full Refresh NiFi Workflow Implementation
Orchestrates complete data warehouse reload with truncate operations
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorEntity, ProcessGroupEntity
from nipyapi.canvas import schedule_process_group

logger = logging.getLogger(__name__)


class FullRefreshWorkflow:
    """Manages full refresh workflow orchestration"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.canvas = nipyapi.canvas
        self.process_group: Optional[ProcessGroupEntity] = None
        self.processors: Dict[str, ProcessorEntity] = {}
        
    def create_workflow(self, parent_pg_id: str) -> ProcessGroupEntity:
        """Create complete full refresh workflow"""
        logger.info("Creating full refresh workflow process group")
        
        # Create main process group
        self.process_group = self.canvas.create_process_group(
            parent_pg=nipyapi.canvas.get_process_group(parent_pg_id, 'id'),
            new_pg_name='wf_FULL_REFRESH',
            location=(0, 0)
        )
        
        pg_id = self.process_group.id
        
        # Build workflow stages
        self._create_pre_refresh_stage(pg_id)
        self._create_backup_stage(pg_id)
        self._create_truncate_staging_stage(pg_id)
        self._create_load_staging_stage(pg_id)
        self._create_truncate_dimensions_stage(pg_id)
        self._create_refresh_dimensions_stage(pg_id)
        self._create_truncate_fact_stage(pg_id)
        self._create_refresh_fact_stage(pg_id)
        self._create_post_refresh_stage(pg_id)
        self._create_error_handling(pg_id)
        
        # Connect all stages
        self._connect_workflow_stages(pg_id)
        
        logger.info(f"Full refresh workflow created: {self.process_group.id}")
        return self.process_group
    
    def _create_pre_refresh_stage(self, pg_id: str):
        """Stage 1: Pre-refresh validation and preparation"""
        logger.info("Creating pre-refresh validation stage")
        
        # Validation processor
        validation_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteScript'),
            location=(100, 100),
            name='Pre_Refresh_Validation',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Script Engine': 'python',
                    'Script Body': self._get_validation_script(),
                    'Script File': '',
                    'Module Directory': ''
                },
                scheduling_period='0 sec',
                auto_terminated_relationships=['failure']
            )
        )
        self.processors['pre_validation'] = validation_proc
        
        # Check disk space
        disk_check_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteStreamCommand'),
            location=(100, 250),
            name='Check_Disk_Space',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Command': '/bin/bash',
                    'Command Arguments': '-c "df -h | grep ${ARCHIVE_PATH}"',
                    'Ignore STDIN': 'true'
                }
            )
        )
        self.processors['disk_check'] = disk_check_proc
        
    def _create_backup_stage(self, pg_id: str):
        """Stage 2: Full database backup"""
        logger.info("Creating backup stage")
        
        # Archive previous data
        archive_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteStreamCommand'),
            location=(100, 400),
            name='Full_Database_Backup',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Command': '${BACKUP_SCRIPT_PATH}',
                    'Command Arguments': '${ARCHIVE_PATH} full_refresh_${now():format("yyyyMMdd_HHmmss")}',
                    'Ignore STDIN': 'true'
                },
                scheduling_period='0 sec'
            )
        )
        self.processors['full_backup'] = archive_proc
        
        # Verify backup
        verify_backup_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteScript'),
            location=(100, 550),
            name='Verify_Backup',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Script Engine': 'python',
                    'Script Body': self._get_backup_verification_script()
                }
            )
        )
        self.processors['verify_backup'] = verify_backup_proc
        
    def _create_truncate_staging_stage(self, pg_id: str):
        """Stage 3: Truncate all staging tables"""
        logger.info("Creating truncate staging stage")
        
        tables = ['STG_CUSTOMER', 'STG_PRODUCT', 'STG_SALES']
        y_position = 700
        
        for table in tables:
            truncate_proc = self.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteSQL'),
                location=(100 + tables.index(table) * 300, y_position),
                name=f'Truncate_{table}',
                config=nipyapi.nifi.ProcessorConfigDTO(
                    properties={
                        'Database Connection Pooling Service': '${DB_CONNECTION_POOL}',
                        'SQL select query': f'TRUNCATE TABLE {table}',
                        'Max Wait Time': '0 seconds'
                    },
                    scheduling_period='0 sec',
                    auto_terminated_relationships=['success', 'failure']
                )
            )
            self.processors[f'truncate_{table.lower()}'] = truncate_proc
            
    def _create_load_staging_stage(self, pg_id: str):
        """Stage 4: Full load all staging tables"""
        logger.info("Creating load staging stage")
        
        staging_configs = [
            {
                'name': 'STG_CUSTOMER',
                'file_pattern': 'customer_master.csv',
                'table': 'STG_CUSTOMER',
                'x_pos': 100
            },
            {
                'name': 'STG_PRODUCT',
                'file_pattern': 'product_master.csv',
                'table': 'STG_PRODUCT',
                'x_pos': 400
            },
            {
                'name': 'STG_SALES',
                'file_pattern': 'sales_transactions_*.csv',
                'table': 'STG_SALES',
                'x_pos': 700
            }
        ]
        
        for config in staging_configs:
            # Fetch file
            fetch_proc = self.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.GetFile'),
                location=(config['x_pos'], 900),
                name=f'Fetch_{config["name"]}_File',
                config=nipyapi.nifi.ProcessorConfigDTO(
                    properties={
                        'Input Directory': '${SOURCE_FILE_PATH}',
                        'File Filter': config['file_pattern'],
                        'Keep Source File': 'true',
                        'Batch Size': '100'
                    },
                    scheduling_period='0 sec'
                )
            )
            self.processors[f'fetch_{config["name"].lower()}'] = fetch_proc
            
            # Parse CSV
            csv_proc = self.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ConvertRecord'),
                location=(config['x_pos'], 1050),
                name=f'Parse_{config["name"]}_CSV',
                config=nipyapi.nifi.ProcessorConfigDTO(
                    properties={
                        'Record Reader': '${CSV_READER}',
                        'Record Writer': '${JSON_WRITER}',
                        'Include Zero Record FlowFiles': 'false'
                    }
                )
            )
            self.processors[f'parse_{config["name"].lower()}'] = csv_proc
            
            # Add metadata
            metadata_proc = self.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.UpdateRecord'),
                location=(config['x_pos'], 1200),
                name=f'Add_{config["name"]}_Metadata',
                config=nipyapi.nifi.ProcessorConfigDTO(
                    properties={
                        'Record Reader': '${JSON_READER}',
                        'Record Writer': '${JSON_WRITER}',
                        '/LOAD_DATE': '${now():format("yyyy-MM-dd HH:mm:ss")}',
                        '/SOURCE_SYSTEM': 'CSV_FILE',
                        '/REFRESH_TYPE': 'FULL'
                    }
                )
            )
            self.processors[f'metadata_{config["name"].lower()}'] = metadata_proc
            
            # Bulk insert to database
            insert_proc = self.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutDatabaseRecord'),
                location=(config['x_pos'], 1350),
                name=f'Bulk_Insert_{config["name"]}',
                config=nipyapi.nifi.ProcessorConfigDTO(
                    properties={
                        'Record Reader': '${JSON_READER}',
                        'Database Connection Pooling Service': '${DB_CONNECTION_POOL}',
                        'Statement Type': 'INSERT',
                        'Table Name': config['table'],
                        'Translate Field Names': 'true',
                        'Max Batch Size': '100000'
                    },
                    scheduling_period='0 sec',
                    bulletin_level='WARN'
                )
            )
            self.processors[f'insert_{config["name"].lower()}'] = insert_proc
            
    def _create_truncate_dimensions_stage(self, pg_id: str):
        """Stage 5: Truncate dimension tables"""
        logger.info("Creating truncate dimensions stage")
        
        # Disable constraints first
        disable_fk_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteSQL'),
            location=(100, 1550),
            name='Disable_Foreign_Keys',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Database Connection Pooling Service': '${DB_CONNECTION_POOL}',
                    'SQL select query': self._get_disable_constraints_sql(),
                    'Max Wait Time': '0 seconds'
                },
                scheduling_period='0 sec',
                auto_terminated_relationships=['success', 'failure']
            )
        )
        self.processors['disable_fk'] = disable_fk_proc
        
        # Truncate dimensions
        dim_tables = ['DIM_CUSTOMER', 'DIM_PRODUCT', 'DIM_DATE']
        
        for idx, table in enumerate(dim_tables):
            truncate_proc = self.canvas.create_processor(
                parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteSQL'),
                location=(100 + idx * 300, 1700),
                name=f'Truncate_{table}',
                config=nipyapi.nifi.ProcessorConfigDTO(
                    properties={
                        'Database Connection Pooling Service': '${DB_CONNECTION_POOL}',
                        'SQL select query': f'TRUNCATE TABLE {table}',
                        'Max Wait Time': '0 seconds'
                    },
                    scheduling_period='0 sec',
                    auto_terminated_relationships=['success', 'failure']
                )
            )
            self.processors[f'truncate_{table.lower()}'] = truncate_proc
            
    def _create_refresh_dimensions_stage(self, pg_id: str):
        """Stage 6: Full refresh dimension tables"""
        logger.info("Creating refresh dimensions stage")
        
        # Customer dimension
        self._create_customer_dimension_flow(pg_id, 100, 1900)
        
        # Product dimension
        self._create_product_dimension_flow(pg_id, 500, 1900)
        
    def _create_customer_dimension_flow(self, pg_id: str, x_pos: int, y_pos: int):
        """Create customer dimension refresh flow with SCD Type 2"""
        
        # Extract from staging
        extract_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteSQL'),
            location=(x_pos, y_pos),
            name='Extract_STG_Customer',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Database Connection Pooling Service': '${DB_CONNECTION_POOL}',
                    'SQL select query': '''
                        SELECT 
                            CUSTOMER_ID,
                            FIRST_NAME,
                            LAST_NAME,
                            EMAIL,
                            PHONE,
                            ADDRESS,
                            CITY,
                            STATE,
                            ZIP_CODE,
                            COUNTRY,
                            REGISTRATION_DATE,
                            CUSTOMER_TYPE,
                            SOURCE_SYSTEM
                        FROM STG_CUSTOMER
                    ''',
                    'Max Wait Time': '0 seconds',
                    'Normalize Table/Column Names': 'false'
                }
            )
        )
        self.processors['extract_stg_customer'] = extract_proc
        
        # Convert to JSON
        convert_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ConvertAvroToJSON'),
            location=(x_pos, y_pos + 150),
            name='Convert_Customer_To_JSON',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'JSON container options': 'none'
                }
            )
        )
        self.processors['convert_customer_json'] = convert_proc
        
        # Cleanse data
        cleanse_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.JoltTransformJSON'),
            location=(x_pos, y_pos + 300),
            name='Cleanse_Customer_Data',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Jolt Transformation DSL': 'jolt-transform-chain',
                    'Jolt Specification': self._get_customer_cleanse_jolt()
                }
            )
        )
        self.processors['cleanse_customer'] = cleanse_proc
        
        # Generate surrogate key
        key_gen_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteScript'),
            location=(x_pos, y_pos + 450),
            name='Generate_Customer_Key',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Script Engine': 'python',
                    'Script Body': self._get_key_generation_script('CUSTOMER')
                }
            )
        )
        self.processors['gen_customer_key'] = key_gen_proc
        
        # Add SCD Type 2 fields
        scd_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.UpdateRecord'),
            location=(x_pos, y_pos + 600),
            name='Add_SCD_Fields',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Record Reader': '${JSON_READER}',
                    'Record Writer': '${JSON_WRITER}',
                    '/EFFECTIVE_FROM_DATE': '${now():format("yyyy-MM-dd HH:mm:ss")}',
                    '/EFFECTIVE_TO_DATE': '9999-12-31 23:59:59',
                    '/IS_CURRENT': 'Y',
                    '/CREATED_DATE': '${now():format("yyyy-MM-dd HH:mm:ss")}'
                }
            )
        )
        self.processors['scd_customer'] = scd_proc
        
        # Insert to dimension
        insert_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutDatabaseRecord'),
            location=(x_pos, y_pos + 750),
            name='Insert_Customer_Dimension',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Record Reader': '${JSON_READER}',
                    'Database Connection Pooling Service': '${DB_CONNECTION_POOL}',
                    'Statement Type': 'INSERT',
                    'Table Name': 'DIM_CUSTOMER',
                    'Translate Field Names': 'true',
                    'Max Batch Size': '50000'
                }
            )
        )
        self.processors['insert_customer_dim'] = insert_proc
        
    def _create_product_dimension_flow(self, pg_id: str, x_pos: int, y_pos: int):
        """Create product dimension refresh flow with SCD Type 1"""
        
        # Extract from staging
        extract_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteSQL'),
            location=(x_pos, y_pos),
            name='Extract_STG_Product',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Database Connection Pooling Service': '${DB_CONNECTION_POOL}',
                    'SQL select query': '''
                        SELECT 
                            PRODUCT_ID,
                            PRODUCT_NAME,
                            PRODUCT_DESCRIPTION,
                            CATEGORY,
                            SUB_CATEGORY,
                            BRAND,
                            UNIT_PRICE,
                            COST_PRICE,
                            SUPPLIER_ID,
                            SUPPLIER_NAME,
                            STATUS,
                            SOURCE_SYSTEM
                        FROM STG_PRODUCT
                    '''
                }
            )
        )
        self.processors['extract_stg_product'] = extract_proc
        
        # Convert and cleanse
        convert_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ConvertAvroToJSON'),
            location=(x_pos, y_pos + 150),
            name='Convert_Product_To_JSON'
        )
        self.processors['convert_product_json'] = convert_proc
        
        # Cleanse and calculate fields
        transform_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.JoltTransformJSON'),
            location=(x_pos, y_pos + 300),
            name='Transform_Product_Data',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Jolt Transformation DSL': 'jolt-transform-chain',
                    'Jolt Specification': self._get_product_transform_jolt()
                }
            )
        )
        self.processors['transform_product'] = transform_proc
        
        # Generate key
        key_gen_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteScript'),
            location=(x_pos, y_pos + 450),
            name='Generate_Product_Key',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Script Engine': 'python',
                    'Script Body': self._get_key_generation_script('PRODUCT')
                }
            )
        )
        self.processors['gen_product_key'] = key_gen_proc
        
        # Insert to dimension
        insert_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutDatabaseRecord'),
            location=(x_pos, y_pos + 600),
            name='Insert_Product_Dimension',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Record Reader': '${JSON_READER}',
                    'Database Connection Pooling Service': '${DB_CONNECTION_POOL}',
                    'Statement Type': 'INSERT',
                    'Table Name': 'DIM_PRODUCT',
                    'Translate Field Names': 'true',
                    'Max Batch Size': '50000'
                }
            )
        )
        self.processors['insert_product_dim'] = insert_proc
        
    def _create_truncate_fact_stage(self, pg_id: str):
        """Stage 7: Truncate fact table"""
        logger.info("Creating truncate fact stage")
        
        truncate_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteSQL'),
            location=(400, 2800),
            name='Truncate_FACT_SALES',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Database Connection Pooling Service': '${DB_CONNECTION_POOL}',
                    'SQL select query': 'TRUNCATE TABLE FACT_SALES',
                    'Max Wait Time': '0 seconds'
                },
                scheduling_period='0 sec',
                auto_terminated_relationships=['success', 'failure']
            )
        )
        self.processors['truncate_fact'] = truncate_proc
        
    def _create_refresh_fact_stage(self, pg_id: str):
        """Stage 8: Full refresh fact table"""
        logger.info("Creating refresh fact stage")
        
        # Extract sales data
        extract_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteSQL'),
            location=(400, 3000),
            name='Extract_STG_Sales',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Database Connection Pooling Service': '${DB_CONNECTION_POOL}',
                    'SQL select query': '''
                        SELECT 
                            TRANSACTION_ID,
                            TRANSACTION_DATE,
                            CUSTOMER_ID,
                            PRODUCT_ID,
                            QUANTITY,
                            UNIT_PRICE,
                            DISCOUNT_PERCENT,
                            TAX_AMOUNT,
                            TOTAL_AMOUNT,
                            PAYMENT_METHOD,
                            STORE_ID,
                            REGION,
                            SOURCE_SYSTEM
                        FROM STG_SALES
                        ORDER BY TRANSACTION_DATE
                    ''',
                    'Fetch Size': '10000'
                }
            )
        )
        self.processors['extract_stg_sales'] = extract_proc
        
        # Convert to JSON
        convert_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ConvertAvroToJSON'),
            location=(400, 3150),
            name='Convert_Sales_To_JSON'
        )
        self.processors['convert_sales_json'] = convert_proc
        
        # Lookup customer key
        lookup_customer_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.LookupRecord'),
            location=(400, 3300),
            name='Lookup_Customer_Key',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Record Reader': '${JSON_READER}',
                    'Record Writer': '${JSON_WRITER}',
                    'Lookup Service': '${CUSTOMER_LOOKUP_SERVICE}',
                    'Result RecordPath': '/CUSTOMER_KEY',
                    'Routing Strategy': 'Route to \'matched\' if any lookup matches',
                    'Record Result Contents': 'Insert Entire Record',
                    'customer_id': '/CUSTOMER_ID'
                }
            )
        )
        self.processors['lookup_customer'] = lookup_customer_proc
        
        # Lookup product key
        lookup_product_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.LookupRecord'),
            location=(400, 3450),
            name='Lookup_Product_Key',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Record Reader': '${JSON_READER}',
                    'Record Writer': '${JSON_WRITER}',
                    'Lookup Service': '${PRODUCT_LOOKUP_SERVICE}',
                    'Result RecordPath': '/PRODUCT_KEY',
                    'product_id': '/PRODUCT_ID'
                }
            )
        )
        self.processors['lookup_product'] = lookup_product_proc
        
        # Calculate measures
        calculate_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteScript'),
            location=(400, 3600),
            name='Calculate_Measures',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Script Engine': 'python',
                    'Script Body': self._get_calculate_measures_script()
                }
            )
        )
        self.processors['calculate_measures'] = calculate_proc
        
        # Generate surrogate key
        key_gen_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteScript'),
            location=(400, 3750),
            name='Generate_Sales_Key',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Script Engine': 'python',
                    'Script Body': self._get_key_generation_script('SALES')
                }
            )
        )
        self.processors['gen_sales_key'] = key_gen_proc
        
        # Bulk insert to fact
        insert_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutDatabaseRecord'),
            location=(400, 3900),
            name='Bulk_Insert_Sales_Fact',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Record Reader': '${JSON_READER}',
                    'Database Connection Pooling Service': '${DB_CONNECTION_POOL}',
                    'Statement Type': 'INSERT',
                    'Table Name': 'FACT_SALES',
                    'Translate Field Names': 'true',
                    'Max Batch Size': '100000',
                    'Obtain Generated Keys': 'false'
                },
                scheduling_period='0 sec',
                bulletin_level='WARN'
            )
        )
        self.processors['insert_sales_fact'] = insert_proc
        
    def _create_post_refresh_stage(self, pg_id: str):
        """Stage 9: Post-refresh operations"""
        logger.info("Creating post-refresh stage")
        
        # Enable constraints
        enable_fk_proc = self.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(pg_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ExecuteSQL'),
            location=(400, 4100),
            name='Enable_Foreign_Keys',
            config=nipyapi.nifi.ProcessorConfigDTO(
                properties={
                    'Database Connection Pooling Service': '${DB_CONNECTION_POOL}',
                    'SQL select query': self._get_enable_constraints_sql()