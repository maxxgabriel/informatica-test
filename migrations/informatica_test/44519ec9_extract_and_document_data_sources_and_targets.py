import xml.etree.ElementTree as ET
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from pyspark.sql.functions import col, lit, current_timestamp, collect_list, struct, explode
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Any
from collections import defaultdict

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InformaticaMetadataExtractor:
    """
    Extract and document data sources and targets from Informatica XML metadata.
    Generates comprehensive documentation including lineage, connections, and data volumes.
    """
    
    def __init__(self, spark: SparkSession, xml_path: str, output_path: str):
        """
        Initialize the metadata extractor.
        
        Args:
            spark: Active SparkSession
            xml_path: Path to Informatica XML export files
            output_path: Path for output documentation and reports
        """
        self.spark = spark
        self.xml_path = xml_path
        self.output_path = output_path
        self.connections = []
        self.sources = []
        self.targets = []
        self.transformations = []
        self.workflows = []
        self.lineage = []
        
        logger.info("Initialized InformaticaMetadataExtractor")
    
    def parse_xml_files(self) -> None:
        """Parse all Informatica XML export files from the specified directory."""
        logger.info(f"Parsing XML files from {self.xml_path}")
        
        for filename in os.listdir(self.xml_path):
            if filename.endswith('.xml'):
                file_path = os.path.join(self.xml_path, filename)
                try:
                    tree = ET.parse(file_path)
                    root = tree.getroot()
                    
                    # Parse different XML sections
                    self._extract_connections(root, filename)
                    self._extract_sources(root, filename)
                    self._extract_targets(root, filename)
                    self._extract_transformations(root, filename)
                    self._extract_workflows(root, filename)
                    self._build_lineage(root, filename)
                    
                    logger.info(f"Successfully parsed {filename}")
                except Exception as e:
                    logger.error(f"Error parsing {filename}: {str(e)}")
    
    def _extract_connections(self, root: ET.Element, source_file: str) -> None:
        """Extract database and service connection definitions."""
        
        # Database connections
        for conn in root.findall('.//RELATIONALCONNECTION') or root.findall('.//CONNECTION'):
            connection_info = {
                'connection_name': conn.get('NAME', ''),
                'connection_type': conn.get('TYPE', 'RELATIONAL'),
                'database_type': conn.get('DBTYPE', ''),
                'server': conn.get('SERVERNAME', '') or conn.get('HOSTNAME', ''),
                'port': conn.get('PORTNUMBER', ''),
                'database_name': conn.get('DATABASENAME', '') or conn.get('SERVICENAME', ''),
                'schema': conn.get('SCHEMANAME', '') or conn.get('USERNAME', ''),
                'connection_string': conn.get('CONNECTIONSTRING', ''),
                'environment': conn.get('ENVIRONMENT', 'UNKNOWN'),
                'owner': conn.get('OWNER', ''),
                'description': conn.get('DESCRIPTION', ''),
                'created_date': conn.get('CREATEDDATE', ''),
                'modified_date': conn.get('MODIFIEDDATE', ''),
                'source_file': source_file,
                'extracted_timestamp': datetime.now().isoformat()
            }
            self.connections.append(connection_info)
        
        # File connections
        for file_conn in root.findall('.//FILECONNECTION'):
            connection_info = {
                'connection_name': file_conn.get('NAME', ''),
                'connection_type': 'FILE',
                'file_path': file_conn.get('FILEPATH', ''),
                'file_type': file_conn.get('FILETYPE', ''),
                'delimiter': file_conn.get('DELIMITER', ''),
                'encoding': file_conn.get('ENCODING', ''),
                'owner': file_conn.get('OWNER', ''),
                'description': file_conn.get('DESCRIPTION', ''),
                'source_file': source_file,
                'extracted_timestamp': datetime.now().isoformat()
            }
            self.connections.append(connection_info)
        
        # Web service connections
        for ws_conn in root.findall('.//WEBSERVICECONNECTION') or root.findall('.//APICONNECTION'):
            connection_info = {
                'connection_name': ws_conn.get('NAME', ''),
                'connection_type': 'WEBSERVICE',
                'endpoint_url': ws_conn.get('ENDPOINTURL', '') or ws_conn.get('URL', ''),
                'authentication_type': ws_conn.get('AUTHTYPE', ''),
                'protocol': ws_conn.get('PROTOCOL', ''),
                'api_type': ws_conn.get('APITYPE', ''),
                'owner': ws_conn.get('OWNER', ''),
                'description': ws_conn.get('DESCRIPTION', ''),
                'source_file': source_file,
                'extracted_timestamp': datetime.now().isoformat()
            }
            self.connections.append(connection_info)
    
    def _extract_sources(self, root: ET.Element, source_file: str) -> None:
        """Extract all source definitions including tables, files, and APIs."""
        
        # Database sources
        for source in root.findall('.//SOURCE'):
            source_info = {
                'source_name': source.get('NAME', ''),
                'source_type': source.get('SOURCETYPE', 'TABLE'),
                'database_type': source.get('DBDNAME', ''),
                'owner_name': source.get('OWNERNAME', ''),
                'table_name': source.get('TABLENAME', ''),
                'sql_override': source.findtext('SQLOVERRIDE', ''),
                'connection_name': source.get('CONNECTIONNAME', '') or source.get('CONNECTIONREFERENCE', ''),
                'description': source.get('DESCRIPTION', ''),
                'is_partitioned': source.get('ISPARTITIONED', 'NO'),
                'source_file': source_file,
                'extracted_timestamp': datetime.now().isoformat(),
                'columns': []
            }
            
            # Extract column definitions
            for field in source.findall('.//SOURCEFIELD'):
                column_info = {
                    'column_name': field.get('NAME', ''),
                    'datatype': field.get('DATATYPE', ''),
                    'precision': field.get('PRECISION', ''),
                    'scale': field.get('SCALE', ''),
                    'nullable': field.get('NULLABLE', ''),
                    'key_type': field.get('KEYTYPE', ''),
                    'business_name': field.get('BUSINESSNAME', ''),
                    'description': field.get('DESCRIPTION', '')
                }
                source_info['columns'].append(column_info)
            
            self.sources.append(source_info)
        
        # File sources
        for file_source in root.findall('.//FLATFILE') or root.findall('.//FILESOURCE'):
            source_info = {
                'source_name': file_source.get('NAME', ''),
                'source_type': 'FLATFILE',
                'file_type': file_source.get('FILETYPE', ''),
                'file_path': file_source.get('FILEPATH', ''),
                'delimiter': file_source.get('DELIMITER', ''),
                'header_rows': file_source.get('HEADERROWS', '0'),
                'encoding': file_source.get('ENCODING', ''),
                'connection_name': file_source.get('CONNECTIONNAME', ''),
                'source_file': source_file,
                'extracted_timestamp': datetime.now().isoformat(),
                'columns': []
            }
            
            for field in file_source.findall('.//FIELD'):
                column_info = {
                    'column_name': field.get('NAME', ''),
                    'datatype': field.get('DATATYPE', ''),
                    'length': field.get('LENGTH', ''),
                    'position': field.get('POSITION', ''),
                    'description': field.get('DESCRIPTION', '')
                }
                source_info['columns'].append(column_info)
            
            self.sources.append(source_info)
        
        # XML/JSON sources
        for xml_source in root.findall('.//XMLSOURCE') or root.findall('.//JSONSOURCE'):
            source_info = {
                'source_name': xml_source.get('NAME', ''),
                'source_type': xml_source.tag,
                'file_path': xml_source.get('FILEPATH', ''),
                'schema_definition': xml_source.get('SCHEMADEFINITION', ''),
                'xpath_expression': xml_source.get('XPATH', ''),
                'connection_name': xml_source.get('CONNECTIONNAME', ''),
                'source_file': source_file,
                'extracted_timestamp': datetime.now().isoformat()
            }
            self.sources.append(source_info)
    
    def _extract_targets(self, root: ET.Element, source_file: str) -> None:
        """Extract all target definitions including tables and files."""
        
        # Database targets
        for target in root.findall('.//TARGET'):
            target_info = {
                'target_name': target.get('NAME', ''),
                'target_type': target.get('TARGETTYPE', 'TABLE'),
                'database_type': target.get('DBDNAME', ''),
                'owner_name': target.get('OWNERNAME', ''),
                'table_name': target.get('TABLENAME', ''),
                'connection_name': target.get('CONNECTIONNAME', '') or target.get('CONNECTIONREFERENCE', ''),
                'load_type': target.get('LOADTYPE', 'NORMAL'),
                'truncate_target': target.get('TRUNCATETARGET', 'NO'),
                'bulk_mode': target.get('BULKMODE', 'NO'),
                'update_strategy': target.get('UPDATESTRATEGY', ''),
                'description': target.get('DESCRIPTION', ''),
                'source_file': source_file,
                'extracted_timestamp': datetime.now().isoformat(),
                'columns': []
            }
            
            # Extract target column definitions
            for field in target.findall('.//TARGETFIELD'):
                column_info = {
                    'column_name': field.get('NAME', ''),
                    'datatype': field.get('DATATYPE', ''),
                    'precision': field.get('PRECISION', ''),
                    'scale': field.get('SCALE', ''),
                    'nullable': field.get('NULLABLE', ''),
                    'key_type': field.get('KEYTYPE', ''),
                    'business_name': field.get('BUSINESSNAME', ''),
                    'description': field.get('DESCRIPTION', '')
                }
                target_info['columns'].append(column_info)
            
            self.targets.append(target_info)
        
        # File targets
        for file_target in root.findall('.//FILETARGET'):
            target_info = {
                'target_name': file_target.get('NAME', ''),
                'target_type': 'FILE',
                'file_type': file_target.get('FILETYPE', ''),
                'file_path': file_target.get('FILEPATH', ''),
                'delimiter': file_target.get('DELIMITER', ''),
                'header': file_target.get('HEADER', 'NO'),
                'encoding': file_target.get('ENCODING', ''),
                'connection_name': file_target.get('CONNECTIONNAME', ''),
                'source_file': source_file,
                'extracted_timestamp': datetime.now().isoformat()
            }
            self.targets.append(target_info)
    
    def _extract_transformations(self, root: ET.Element, source_file: str) -> None:
        """Extract transformation logic and data quality rules."""
        
        transformation_types = [
            'AGGREGATOR', 'EXPRESSION', 'FILTER', 'JOINER', 'LOOKUP',
            'NORMALIZER', 'RANKER', 'ROUTER', 'SORTER', 'UPDATESTRATEGY',
            'UNION', 'SEQUENCE', 'STOREDPROCEDURE'
        ]
        
        for trans_type in transformation_types:
            for trans in root.findall(f'.//{trans_type}') or root.findall('.//TRANSFORMATION[@TYPE="' + trans_type + '"]'):
                trans_info = {
                    'transformation_name': trans.get('NAME', ''),
                    'transformation_type': trans_type,
                    'description': trans.get('DESCRIPTION', ''),
                    'reusable': trans.get('REUSABLE', 'NO'),
                    'source_file': source_file,
                    'extracted_timestamp': datetime.now().isoformat(),
                    'properties': {},
                    'expressions': [],
                    'conditions': []
                }
                
                # Extract transformation-specific properties
                if trans_type == 'FILTER':
                    filter_cond = trans.findtext('.//FILTERCONDITION') or trans.get('FILTERCONDITION', '')
                    trans_info['conditions'].append({'type': 'FILTER', 'expression': filter_cond})
                
                elif trans_type == 'EXPRESSION':
                    for port in trans.findall('.//TRANSFORMFIELD'):
                        if port.get('PORTTYPE', '') == 'OUTPUT':
                            expr = {
                                'output_field': port.get('NAME', ''),
                                'datatype': port.get('DATATYPE', ''),
                                'expression': port.get('EXPRESSION', ''),
                                'description': port.get('DESCRIPTION', '')
                            }
                            trans_info['expressions'].append(expr)
                
                elif trans_type == 'JOINER':
                    trans_info['properties'] = {
                        'join_type': trans.get('JOINTYPE', ''),
                        'join_condition': trans.get('JOINCONDITION', ''),
                        'master_source': trans.get('MASTERSOURCE', ''),
                        'detail_source': trans.get('DETAILSOURCE', '')
                    }
                
                elif trans_type == 'LOOKUP':
                    trans_info['properties'] = {
                        'lookup_table': trans.get('LOOKUPTABLE', ''),
                        'connection': trans.get('CONNECTIONNAME', ''),
                        'cache_type': trans.get('CACHETYPE', ''),
                        'lookup_condition': trans.get('LOOKUPCONDITION', '')
                    }
                
                elif trans_type == 'AGGREGATOR':
                    trans_info['properties'] = {
                        'group_by_ports': trans.get('GROUPBYPORTS', ''),
                        'sorted_input': trans.get('SORTEDINPUT', 'NO')
                    }
                    for port in trans.findall('.//TRANSFORMFIELD'):
                        if port.get('AGGREGATE', '') == 'YES':
                            expr = {
                                'aggregate_field': port.get('NAME', ''),
                                'aggregate_expression': port.get('EXPRESSION', '')
                            }
                            trans_info['expressions'].append(expr)
                
                self.transformations.append(trans_info)
    
    def _extract_workflows(self, root: ET.Element, source_file: str) -> None:
        """Extract workflow definitions including scheduling and dependencies."""
        
        for workflow in root.findall('.//WORKFLOW'):
            workflow_info = {
                'workflow_name': workflow.get('NAME', ''),
                'description': workflow.get('DESCRIPTION', ''),
                'folder_name': workflow.get('FOLDERNAME', ''),
                'is_valid': workflow.get('ISVALID', 'YES'),
                'concurrent_run': workflow.get('CONCURRENTRUN', 'NO'),
                'owner': workflow.get('OWNER', ''),
                'created_date': workflow.get('CREATEDDATE', ''),
                'modified_date': workflow.get('MODIFIEDDATE', ''),
                'source_file': source_file,
                'extracted_timestamp': datetime.now().isoformat(),
                'tasks': [],
                'schedule': {}
            }
            
            # Extract scheduler information
            scheduler = workflow.find('.//SCHEDULER')
            if scheduler is not None:
                workflow_info['schedule'] = {
                    'schedule_type': scheduler.get('SCHEDULETYPE', ''),
                    'start_time': scheduler.get('STARTTIME', ''),
                    'end_time': scheduler.get('ENDTIME', ''),
                    'frequency': scheduler.get('FREQUENCY', ''),
                    'interval': scheduler.get('INTERVAL', ''),
                    'days_of_week': scheduler.get('DAYSOFWEEK', ''),
                    'is_enabled': scheduler.get('ISENABLED', 'NO')
                }
            
            # Extract session tasks
            for session in workflow.findall('.//SESSION'):
                task_info = {
                    'task_name': session.get('NAME', ''),
                    'task_type': 'SESSION',
                    'mapping_name': session.get('MAPPINGNAME', ''),
                    'reusable': session.get('REUSABLE', 'NO'),
                    'start_time': session.get('STARTTIME', ''),
                    'end_time': session.get('ENDTIME', ''),
                    'status': session.get('STATUS', ''),
                    'rows_processed': session.get('ROWSPROCESSED', '0'),
                    'rows_rejected': session.get('ROWSREJECTED', '0')
                }
                workflow_info['tasks'].append(task_info)
            
            # Extract other task types
            for task in workflow.findall('.//TASK'):
                task_info = {
                    'task_name': task.get('NAME', ''),
                    'task_type': task.get('TYPE', ''),
                    'description': task.get('DESCRIPTION', ''),
                    'command': task.get('COMMAND', ''),
                    'dependencies': []
                }
                
                # Extract task dependencies
                for link in workflow.findall(f'.//TASKLINK[@TOTASK="{task.get("NAME", "")}"]'):
                    dependency = {
                        'from_task': link.get('FROMTASK', ''),
                        'condition': link.get('CONDITION', '')
                    }
                    task_info['dependencies'].append(dependency)
                
                workflow_info['tasks'].append(task_info)
            
            self.workflows.append(workflow_info)
    
    def _build_lineage(self, root: ET.Element, source_file: str) -> None:
        """Build source-to-target data lineage."""
        
        for mapping in root.findall('.//MAPPING'):
            mapping_name = mapping.get('NAME', '')
            
            # Track data flow through mapping
            for instance in mapping.findall('.//INSTANCE'):
                instance_name = instance.get('NAME', '')
                instance_type = instance.get('TYPE', '')
                transformation_name = instance.get('TRANSFORMATION_NAME', '')
                
                # Extract connector information
                for connector in mapping.findall(f'.//CONNECTOR[@FROMINSTANCE="{instance_name}"]'):
                    lineage_entry = {
                        'mapping_name': mapping_name,
                        'from_instance': connector.get('FROMINSTANCE', ''),
                        'from_field': connector.get('FROMFIELD', ''),
                        'to_instance': connector.get('TOINSTANCE', ''),
                        'to_field': connector.get('TOFIELD', ''),
                        'transformation_type': instance_type,
                        'transformation_name': transformation_name,
                        'source_file': source_file,
                        'extracted_timestamp': datetime.now().isoformat()
                    }
                    self.lineage.append(lineage_entry)
    
    def create_dataframes(self) -> Dict[str, Any]:
        """Convert extracted metadata to PySpark DataFrames."""
        logger.info("Creating PySpark DataFrames from extracted metadata")
        
        # Connections DataFrame
        connections_schema = StructType([
            StructField("connection_name", StringType(), True),
            StructField("connection_type", StringType(), True),
            StructField("database_type", StringType(), True),
            StructField("server", StringType(), True),
            StructField("port", StringType(), True),
            StructField("database_name", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("connection_string", StringType(), True),
            StructField("environment", StringType(), True),
            StructField("owner", StringType(), True),
            StructField("description", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("extracted_timestamp", StringType(), True)
        ])
        
        df_connections = self.spark.createDataFrame(
            [self._normalize_dict(c, connections_schema) for c in self.connections],
            schema=connections_schema
        ) if self.connections else self.spark.createDataFrame([], connections_schema)
        
        # Sources DataFrame
        sources_flat = []
        for source in self.sources:
            source_copy = source.copy()
            columns = source_copy.pop('columns', [])
            source_copy['column_count'] = len(columns)
            source_copy['columns_json'] = json.dumps(columns)
            sources_flat.append(source_copy)
        
        df_sources = self.spark.createDataFrame(sources_flat) if sources_flat else None
        
        # Targets DataFrame
        targets_flat = []
        for target in self.targets:
            target_copy = target.copy()
            columns = target_copy.pop('columns', [])
            target_copy['column_count'] = len(columns)
            target_copy['columns_json'] = json.dumps(columns)
            targets_flat.append(target_copy)
        
        df_targets = self.spark.createDataFrame(targets_flat) if targets_flat else None
        
        # Transformations DataFrame
        transformations_flat = []
        for trans in self.transformations:
            trans_copy = trans.copy()
            trans_copy['properties_json'] = json.dumps(trans_copy.pop('properties', {}))
            trans_copy['expressions_json'] = json.dumps(trans_copy.pop('expressions', []))
            trans_copy['conditions_json'] = json.dumps(trans_copy.pop('conditions', []))
            transformations_flat.append(trans_copy)
        
        df_transformations = self.spark.createDataFrame(transformations_flat) if transformations_flat else None
        
        # Workflows DataFrame
        workflows_flat = []
        for workflow in self.workflows:
            workflow_copy = workflow.copy()
            workflow_copy['tasks_json'] = json.dumps(workflow_copy.pop('tasks', []))
            workflow_copy['schedule_json'] = json.dumps(workflow_copy.pop('schedule', {}))
            workflow_copy['task_count'] = len(workflow.get('tasks', []))
            workflows_flat.append(workflow_copy)
        
        df_workflows = self.spark.createDataFrame(workflows_flat) if workflows_flat else None
        
        # Lineage DataFrame
        df_lineage = self.spark.createDataFrame(self.lineage) if self.lineage else None
        
        return {
            'connections': df_connections,
            'sources': df_sources,
            'targets': df_targets,
            'transformations': df_transformations,
            'workflows': df_workflows,
            'lineage': df_lineage
        }
    
    def _normalize_dict(self, data: Dict, schema: StructType) -> Dict:
        """Normalize dictionary to match schema fields."""
        field_names = [field.name for field in schema.fields]
        return {k: data.get(k) for k in field_names}
    
    def generate_inventory_reports(self, dataframes: Dict[str, Any]) -> None:
        """Generate comprehensive inventory reports."""
        logger.info("Generating inventory reports")
        
        # Connections inventory
        if dataframes['connections']:
            df_conn_summary = dataframes['connections'].groupBy(
                'connection_type', 'database_type', 'environment'
            ).count().orderBy('connection_type', 'database_type')
            
            df_conn_summary.write.mode('overwrite').parquet(
                f"{self.output_path}/reports/connections_inventory"
            )
            
            # Detailed connections
            dataframes['connections'].write.mode('overwrite').parquet(
                f"{self.output_path}/reports/connections_detailed"
            )
        
        # Sources inventory
        if dataframes['sources']:
            df_sources_summary = dataframes['sources'].groupBy(
                'source_type', 'database_type'
            ).count().orderBy('source_type')
            
            df_sources_summary.write.mode('overwrite').parquet(
                f"{self.output_path}/reports/sources_inventory"
            )
            
            dataframes['sources'].write.mode('overwrite').parquet(
                f"{self.output_path}/reports/sources_detailed"
            )
        
        # Targets inventory
        if dataframes['targets']:
            df_targets_summary = dataframes['targets'].groupBy(
                'target_type', 'database_type', 'load_type'
            ).count().orderBy('target_type')
            
            df_targets_summary.write.mode('overwrite').parquet(
                f"{self.output_path}/reports/targets_inventory"
            )
            
            dataframes['targets'].write.mode('overwrite').parquet(
                f"{self.output_path}/reports/targets_detailed"
            )
        
        # Transformations inventory
        if dataframes['transformations']:
            df_trans_summary = dataframes['transformations'].groupBy(
                'transformation_type', 'reusable'
            ).count().orderBy('transformation_type')
            
            df_trans_summary.write.mode('overwrite').parquet(
                f"{self.output_path}/reports/transformations_inventory"
            )
            
            dataframes['transformations'].write.mode('overwrite').parquet(
                f"{self.output_path}/reports/transformations_detailed"
            )
        
        # Workflows inventory
        if dataframes['workflows']:
            df_workflow_summary = dataframes['workflows'].groupBy(
                'folder_name', 'is_valid'
            ).count().orderBy('folder_name')
            
            df_workflow_summary.write.mode('overwrite').parquet(
                f"{self.output_path}/reports/workflows_inventory"
            )
            
            dataframes['workflows'].write.mode('overwrite').parquet(
                f"{self.output_path}/reports/workflows_detailed"
            )
    
    def generate_lineage_report(self, dataframes: Dict[str, Any]) -> None:
        """Generate source-to-target lineage documentation."""
        logger.info("Generating lineage reports")
        
        if dataframes['lineage']:
            # Direct lineage paths
            dataframes['lineage'].write.mode('overwrite').parquet(
                f"{self.output_path}/reports/lineage_detailed"
            )
            
            # Aggregated lineage by mapping
            df_lineage_summary = dataframes['lineage'].groupBy(
                'mapping_name', 'transformation_type'
            ).count().orderBy('mapping_name')
            
            df_lineage_summary.write.mode('overwrite').parquet(
                f"{self.output_path}/reports/lineage_summary"
            )
            
            # Source to target mapping
            if dataframes['sources'] and dataframes['targets']:
                df_lineage_expanded = dataframes['lineage'].join(
                    dataframes['sources'].select(
                        col('source_name').alias('src_name'),
                        col('connection_name').alias('src_connection')
                    ),
                    dataframes['lineage']['from_instance'] == col('src_name'),
                    'left'
                ).join(
                    dataframes['targets'].select(
                        col('target_name').alias('tgt_name'),
                        col('connection_name').alias('tgt_connection')
                    ),
                    dataframes['lineage']['to_instance'] == col('tgt_name'),
                    'left'
                )
                
                df_lineage_expanded.write.mode('overwrite').parquet(
                    f"{self.output_path}/reports/lineage_source_to_target"
                )
    
    def generate_data_volumes_report(self, dataframes: Dict[str, Any]) -> None:
        """Generate data volumes and refresh frequency documentation."""
        logger.info("Generating data volumes report")
        
        # Workflow execution statistics
        if dataframes['workflows']:
            df_volumes = dataframes['workflows'].select(
                'workflow_name',
                'folder_name',
                'schedule_json'
            )
            
            df_volumes.write.mode('overwrite').parquet(
                f"{self.output_path}/reports/data_volumes_and_schedules"
            )
    
    def generate_integration_points_catalog(self, dataframes: Dict[str, Any]) -> None:
        """Generate catalog of API and web service integration points."""
        logger.info("Generating integration points catalog")
        
        if dataframes['connections']:
            df_integrations = dataframes['connections'].filter(
                col('connection_type').isin(['WEBSERVICE', 'API', 'REST', 'SOAP'])
            )
            
            if df_integrations.count() > 0:
                df_integrations.write.mode('overwrite').parquet(
                    f"{self.output_path}/reports/integration_points_catalog"
                )
    
    def generate_data_quality_rules(self, dataframes: Dict[str, Any]) -> None:
        """Document data quality rules and constraints."""
        logger.info("Generating data quality rules documentation")
        
        if dataframes['transformations']:
            df_dq_rules = dataframes['transformations'].filter(
                col('transformation_type').isin(['FILTER', 'EXPRESSION', 'ROUTER'])
            ).select(
                'transformation_name',
                'transformation_type',
                'conditions_json',
                'expressions_json',
                'description'
            )
            
            if df_dq_rules.count() > 0:
                df_dq_rules.write.mode('overwrite').parquet(
                    f"{self.output_path}/reports/data_quality_rules"
                )
    
    def generate_master_documentation(self, dataframes: Dict[str, Any]) -> None:
        """Generate master documentation summary."""
        logger.info("Generating master documentation")
        
        summary_data = []
        
        for name, df in dataframes.items():
            if df:
                count = df.count()
                columns = len(df.columns)
                summary_data.append({
                    'artifact_type': name,
                    'total_count': count,
                    'column_count': columns,
                    'generated_timestamp': datetime.now().isoformat()
                })
        
        if summary_data:
            df_summary = self.spark.createDataFrame(summary_data)
            df_summary.write.mode('overwrite').parquet(
                f"{self.output_path}/reports/master_documentation_summary"
            )
    
    def export_to_json(self, data