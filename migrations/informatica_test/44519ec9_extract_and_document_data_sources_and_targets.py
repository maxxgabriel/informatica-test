import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
import os
from typing import Dict, List, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InformaticaSourceTargetExtractor:
    """
    Extract and document data sources and targets from Informatica workflows.
    Analyzes XML definitions and creates comprehensive data lineage documentation.
    """
    
    def __init__(self, spark: SparkSession, xml_path: str, output_path: str):
        """
        Initialize the extractor with Spark session and paths.
        
        Args:
            spark: Active SparkSession
            xml_path: Path to Informatica XML files
            output_path: Path for output documentation
        """
        self.spark = spark
        self.xml_path = xml_path
        self.output_path = output_path
        self.sources_inventory = []
        self.targets_inventory = []
        self.lineage_mappings = []
        self.integration_points = []
        
    def extract_source_connections(self, xml_file_path: str) -> List[Dict]:
        """
        Extract source connection definitions from Informatica XML.
        
        Args:
            xml_file_path: Path to Informatica XML file
            
        Returns:
            List of source connection dictionaries
        """
        logger.info(f"Extracting source connections from: {xml_file_path}")
        
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            connections = []
            
            # Extract SOURCE elements
            for source in root.findall('.//SOURCE'):
                connection_info = {
                    'source_id': source.get('DATABASETYPE', 'Unknown'),
                    'source_name': source.get('NAME', 'Unknown'),
                    'database_type': source.get('DATABASETYPE', 'Unknown'),
                    'owner_name': source.get('OWNERNAME', ''),
                    'db_name': source.get('DBDNAME', ''),
                    'source_type': source.get('SOURCETYPE', 'Definition'),
                    'description': source.get('DESCRIPTION', ''),
                    'extraction_timestamp': datetime.now().isoformat()
                }
                
                # Extract table/source fields
                fields = []
                for field in source.findall('.//SOURCEFIELD'):
                    field_info = {
                        'field_name': field.get('NAME', ''),
                        'datatype': field.get('DATATYPE', ''),
                        'precision': field.get('PRECISION', ''),
                        'scale': field.get('SCALE', ''),
                        'nullable': field.get('NULLABLE', 'NULL'),
                        'key_type': field.get('KEYTYPE', 'NOT A KEY')
                    }
                    fields.append(field_info)
                
                connection_info['fields'] = fields
                connection_info['field_count'] = len(fields)
                connections.append(connection_info)
            
            # Extract relational connections
            for rel_conn in root.findall('.//RELATIONALCONNECTION'):
                connection_info = {
                    'connection_name': rel_conn.get('NAME', 'Unknown'),
                    'connection_type': rel_conn.get('TYPE', 'Unknown'),
                    'database_type': rel_conn.get('DBTYPE', 'Unknown'),
                    'username': rel_conn.get('USERNAME', ''),
                    'connection_string': rel_conn.get('CONNECTIONSTRING', ''),
                    'server_name': rel_conn.get('SERVERNAME', ''),
                    'extraction_timestamp': datetime.now().isoformat()
                }
                connections.append(connection_info)
            
            self.sources_inventory.extend(connections)
            logger.info(f"Extracted {len(connections)} source connections")
            return connections
            
        except Exception as e:
            logger.error(f"Error extracting source connections: {str(e)}")
            raise
    
    def extract_target_connections(self, xml_file_path: str) -> List[Dict]:
        """
        Extract target connection definitions and write patterns.
        
        Args:
            xml_file_path: Path to Informatica XML file
            
        Returns:
            List of target connection dictionaries
        """
        logger.info(f"Extracting target connections from: {xml_file_path}")
        
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            targets = []
            
            # Extract TARGET elements
            for target in root.findall('.//TARGET'):
                target_info = {
                    'target_id': target.get('NAME', 'Unknown'),
                    'target_name': target.get('NAME', 'Unknown'),
                    'database_type': target.get('DATABASETYPE', 'Unknown'),
                    'owner_name': target.get('OWNERNAME', ''),
                    'table_name': target.get('NAME', ''),
                    'target_type': target.get('CONSTRAINT', 'INSERT'),
                    'description': target.get('DESCRIPTION', ''),
                    'extraction_timestamp': datetime.now().isoformat()
                }
                
                # Extract target fields
                fields = []
                for field in target.findall('.//TARGETFIELD'):
                    field_info = {
                        'field_name': field.get('NAME', ''),
                        'datatype': field.get('DATATYPE', ''),
                        'precision': field.get('PRECISION', ''),
                        'scale': field.get('SCALE', ''),
                        'nullable': field.get('NULLABLE', 'NULL'),
                        'key_type': field.get('KEYTYPE', 'NOT A KEY')
                    }
                    fields.append(field_info)
                
                target_info['fields'] = fields
                target_info['field_count'] = len(fields)
                targets.append(target_info)
            
            # Extract target load order and write patterns
            for target_load in root.findall('.//TARGETLOADORDER'):
                load_order_info = {
                    'target_instance': target_load.get('TARGETINSTANCE', ''),
                    'order': target_load.get('ORDER', ''),
                    'load_type': 'SEQUENTIAL' if target_load.get('ORDER') else 'PARALLEL'
                }
                
                # Match with target and update
                for target in targets:
                    if target['target_name'] == load_order_info['target_instance']:
                        target['load_order'] = load_order_info['order']
                        target['load_type'] = load_order_info['load_type']
            
            self.targets_inventory.extend(targets)
            logger.info(f"Extracted {len(targets)} target connections")
            return targets
            
        except Exception as e:
            logger.error(f"Error extracting target connections: {str(e)}")
            raise
    
    def identify_file_sources(self, xml_file_path: str) -> List[Dict]:
        """
        Identify flat files, XML, JSON, and other file-based sources.
        
        Args:
            xml_file_path: Path to Informatica XML file
            
        Returns:
            List of file source dictionaries
        """
        logger.info(f"Identifying file sources from: {xml_file_path}")
        
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            file_sources = []
            
            # Extract file sources
            for source in root.findall('.//SOURCE'):
                db_type = source.get('DATABASETYPE', '')
                
                if db_type in ['Flat File', 'FILE', 'FLATFILE']:
                    file_info = {
                        'source_name': source.get('NAME', 'Unknown'),
                        'file_type': 'FLAT_FILE',
                        'delimiter': source.get('DELIMITER', ','),
                        'header_option': source.get('HEADERLINEOPTIONS', 'NONE'),
                        'file_format': source.get('FILEFORMAT', 'DELIMITED'),
                        'owner_name': source.get('OWNERNAME', ''),
                        'description': source.get('DESCRIPTION', ''),
                        'extraction_timestamp': datetime.now().isoformat()
                    }
                    
                    # Extract field definitions
                    fields = []
                    for field in source.findall('.//SOURCEFIELD'):
                        field_info = {
                            'field_name': field.get('NAME', ''),
                            'datatype': field.get('DATATYPE', ''),
                            'precision': field.get('PRECISION', ''),
                            'position': field.get('FIELDPROPERTY', '')
                        }
                        fields.append(field_info)
                    
                    file_info['fields'] = fields
                    file_info['field_count'] = len(fields)
                    file_sources.append(file_info)
                
                elif db_type in ['XML', 'JSON']:
                    file_info = {
                        'source_name': source.get('NAME', 'Unknown'),
                        'file_type': db_type,
                        'schema_definition': source.get('XMLSCHEMA', ''),
                        'description': source.get('DESCRIPTION', ''),
                        'extraction_timestamp': datetime.now().isoformat()
                    }
                    file_sources.append(file_info)
            
            self.sources_inventory.extend(file_sources)
            logger.info(f"Identified {len(file_sources)} file sources")
            return file_sources
            
        except Exception as e:
            logger.error(f"Error identifying file sources: {str(e)}")
            raise
    
    def extract_data_volumes_frequencies(self, xml_file_path: str) -> List[Dict]:
        """
        Document data volumes and refresh frequencies from workflow definitions.
        
        Args:
            xml_file_path: Path to Informatica XML file
            
        Returns:
            List of volume and frequency dictionaries
        """
        logger.info(f"Extracting data volumes and frequencies from: {xml_file_path}")
        
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            volume_info = []
            
            # Extract workflow scheduling information
            for workflow in root.findall('.//WORKFLOW'):
                workflow_info = {
                    'workflow_name': workflow.get('NAME', 'Unknown'),
                    'description': workflow.get('DESCRIPTION', ''),
                    'is_valid': workflow.get('ISVALID', 'YES'),
                    'version_number': workflow.get('VERSIONNUMBER', '1'),
                    'extraction_timestamp': datetime.now().isoformat()
                }
                
                # Extract scheduler information
                for scheduler in workflow.findall('.//SCHEDULER'):
                    schedule_info = {
                        'schedule_type': scheduler.get('TYPE', 'ON_DEMAND'),
                        'start_time': scheduler.get('STARTTIME', ''),
                        'end_time': scheduler.get('ENDTIME', ''),
                        'repeat_interval': scheduler.get('REPEATINTERVAL', ''),
                        'frequency': self._determine_frequency(scheduler.get('REPEATINTERVAL', ''))
                    }
                    workflow_info.update(schedule_info)
                
                # Extract session information for volume estimates
                for session in workflow.findall('.//SESSION'):
                    session_info = {
                        'session_name': session.get('NAME', ''),
                        'mapping_name': session.get('MAPPINGNAME', ''),
                        'sort_order_flag': session.get('SORTORDERFLG', 'NO')
                    }
                    
                    # Extract session properties for performance hints
                    for session_config in session.findall('.//SESSIONCONFIG'):
                        commit_interval = session_config.get('COMMITINTERVAL', '10000')
                        session_info['commit_interval'] = commit_interval
                        session_info['estimated_volume_hint'] = 'HIGH' if int(commit_interval) > 50000 else 'MEDIUM'
                    
                    workflow_info['session_info'] = session_info
                
                volume_info.append(workflow_info)
            
            logger.info(f"Extracted {len(volume_info)} workflow volume/frequency definitions")
            return volume_info
            
        except Exception as e:
            logger.error(f"Error extracting volumes and frequencies: {str(e)}")
            raise
    
    def identify_api_web_services(self, xml_file_path: str) -> List[Dict]:
        """
        Identify APIs and web services integrations.
        
        Args:
            xml_file_path: Path to Informatica XML file
            
        Returns:
            List of API/web service integration dictionaries
        """
        logger.info(f"Identifying API and web services from: {xml_file_path}")
        
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            integrations = []
            
            # Extract web service sources/targets
            for ws_source in root.findall('.//WEBSERVICESOURCE'):
                ws_info = {
                    'integration_type': 'WEB_SERVICE_SOURCE',
                    'name': ws_source.get('NAME', 'Unknown'),
                    'wsdl_url': ws_source.get('WSDLURL', ''),
                    'operation': ws_source.get('OPERATION', ''),
                    'endpoint': ws_source.get('ENDPOINT', ''),
                    'description': ws_source.get('DESCRIPTION', ''),
                    'extraction_timestamp': datetime.now().isoformat()
                }
                integrations.append(ws_info)
            
            for ws_target in root.findall('.//WEBSERVICETARGET'):
                ws_info = {
                    'integration_type': 'WEB_SERVICE_TARGET',
                    'name': ws_target.get('NAME', 'Unknown'),
                    'wsdl_url': ws_target.get('WSDLURL', ''),
                    'operation': ws_target.get('OPERATION', ''),
                    'endpoint': ws_target.get('ENDPOINT', ''),
                    'description': ws_target.get('DESCRIPTION', ''),
                    'extraction_timestamp': datetime.now().isoformat()
                }
                integrations.append(ws_info)
            
            # Extract JMS sources/targets
            for jms_source in root.findall('.//JMSSOURCE'):
                jms_info = {
                    'integration_type': 'JMS_SOURCE',
                    'name': jms_source.get('NAME', 'Unknown'),
                    'connection_name': jms_source.get('CONNECTIONNAME', ''),
                    'queue_name': jms_source.get('QUEUENAME', ''),
                    'description': jms_source.get('DESCRIPTION', ''),
                    'extraction_timestamp': datetime.now().isoformat()
                }
                integrations.append(jms_info)
            
            self.integration_points.extend(integrations)
            logger.info(f"Identified {len(integrations)} API/web service integrations")
            return integrations
            
        except Exception as e:
            logger.error(f"Error identifying API/web services: {str(e)}")
            raise
    
    def create_source_target_lineage(self, xml_file_path: str) -> List[Dict]:
        """
        Create source-to-target lineage mappings.
        
        Args:
            xml_file_path: Path to Informatica XML file
            
        Returns:
            List of lineage mapping dictionaries
        """
        logger.info(f"Creating source-to-target lineage from: {xml_file_path}")
        
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            lineage_maps = []
            
            # Extract mappings
            for mapping in root.findall('.//MAPPING'):
                mapping_name = mapping.get('NAME', 'Unknown')
                mapping_desc = mapping.get('DESCRIPTION', '')
                
                # Extract source qualifiers and their sources
                source_instances = {}
                for sq in mapping.findall('.//SOURCEQUAL'):
                    sq_name = sq.get('NAME', '')
                    source_name = sq.get('SOURCENAME', '')
                    source_instances[sq_name] = source_name
                
                # Extract transformations
                transformations = []
                for transform in mapping.findall('.//TRANSFORMATION'):
                    transform_info = {
                        'transform_name': transform.get('NAME', ''),
                        'transform_type': transform.get('TYPE', ''),
                        'description': transform.get('DESCRIPTION', '')
                    }
                    transformations.append(transform_info)
                
                # Extract target instances
                target_instances = []
                for target_inst in mapping.findall('.//TARGETINSTANCE'):
                    target_info = {
                        'target_name': target_inst.get('NAME', ''),
                        'instance_name': target_inst.get('INSTANCENAME', '')
                    }
                    target_instances.append(target_info)
                
                # Extract connectors to build lineage
                for connector in mapping.findall('.//CONNECTOR'):
                    from_instance = connector.get('FROMINSTANCE', '')
                    to_instance = connector.get('TOINSTANCE', '')
                    from_field = connector.get('FROMFIELD', '')
                    to_field = connector.get('TOFIELD', '')
                    
                    # Resolve source
                    source_name = source_instances.get(from_instance, from_instance)
                    
                    lineage_entry = {
                        'mapping_name': mapping_name,
                        'mapping_description': mapping_desc,
                        'source_system': source_name,
                        'source_field': from_field,
                        'target_system': to_instance,
                        'target_field': to_field,
                        'transformations': transformations,
                        'extraction_timestamp': datetime.now().isoformat()
                    }
                    lineage_maps.append(lineage_entry)
            
            self.lineage_mappings.extend(lineage_maps)
            logger.info(f"Created {len(lineage_maps)} lineage mappings")
            return lineage_maps
            
        except Exception as e:
            logger.error(f"Error creating lineage mappings: {str(e)}")
            raise
    
    def extract_data_quality_rules(self, xml_file_path: str) -> List[Dict]:
        """
        Document data quality rules and constraints.
        
        Args:
            xml_file_path: Path to Informatica XML file
            
        Returns:
            List of data quality rule dictionaries
        """
        logger.info(f"Extracting data quality rules from: {xml_file_path}")
        
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            dq_rules = []
            
            # Extract expression transformations (common for DQ rules)
            for expr_transform in root.findall('.//TRANSFORMATION[@TYPE="Expression"]'):
                transform_name = expr_transform.get('NAME', '')
                
                for transform_field in expr_transform.findall('.//TRANSFORMFIELD'):
                    field_name = transform_field.get('NAME', '')
                    expression = transform_field.get('EXPRESSION', '')
                    datatype = transform_field.get('DATATYPE', '')
                    
                    if expression:  # If there's a transformation logic
                        dq_rule = {
                            'transformation_name': transform_name,
                            'field_name': field_name,
                            'rule_type': 'TRANSFORMATION_RULE',
                            'expression': expression,
                            'datatype': datatype,
                            'extraction_timestamp': datetime.now().isoformat()
                        }
                        dq_rules.append(dq_rule)
            
            # Extract filter transformations (for data filtering)
            for filter_transform in root.findall('.//TRANSFORMATION[@TYPE="Filter"]'):
                transform_name = filter_transform.get('NAME', '')
                
                for transform_field in filter_transform.findall('.//TRANSFORMFIELD'):
                    expression = transform_field.get('EXPRESSION', '')
                    
                    if expression:
                        dq_rule = {
                            'transformation_name': transform_name,
                            'rule_type': 'FILTER_RULE',
                            'filter_condition': expression,
                            'extraction_timestamp': datetime.now().isoformat()
                        }
                        dq_rules.append(dq_rule)
            
            # Extract lookup transformations (for validation)
            for lookup_transform in root.findall('.//TRANSFORMATION[@TYPE="Lookup"]'):
                transform_name = lookup_transform.get('NAME', '')
                
                dq_rule = {
                    'transformation_name': transform_name,
                    'rule_type': 'LOOKUP_VALIDATION',
                    'lookup_table': lookup_transform.get('LOOKUPTABLE', ''),
                    'extraction_timestamp': datetime.now().isoformat()
                }
                dq_rules.append(dq_rule)
            
            # Extract target constraints
            for target in root.findall('.//TARGET'):
                target_name = target.get('NAME', '')
                
                for target_field in target.findall('.//TARGETFIELD'):
                    field_name = target_field.get('NAME', '')
                    nullable = target_field.get('NULLABLE', '')
                    key_type = target_field.get('KEYTYPE', '')
                    
                    if key_type != 'NOT A KEY' or nullable == 'NOTNULL':
                        dq_rule = {
                            'target_name': target_name,
                            'field_name': field_name,
                            'rule_type': 'CONSTRAINT',
                            'nullable': nullable,
                            'key_type': key_type,
                            'extraction_timestamp': datetime.now().isoformat()
                        }
                        dq_rules.append(dq_rule)
            
            logger.info(f"Extracted {len(dq_rules)} data quality rules")
            return dq_rules
            
        except Exception as e:
            logger.error(f"Error extracting data quality rules: {str(e)}")
            raise
    
    def _determine_frequency(self, repeat_interval: str) -> str:
        """
        Determine human-readable frequency from repeat interval.
        
        Args:
            repeat_interval: Repeat interval string
            
        Returns:
            Frequency description
        """
        if not repeat_interval:
            return 'ON_DEMAND'
        
        interval_minutes = int(repeat_interval) if repeat_interval.isdigit() else 0
        
        if interval_minutes == 0:
            return 'ON_DEMAND'
        elif interval_minutes < 60:
            return f'EVERY_{interval_minutes}_MINUTES'
        elif interval_minutes == 60:
            return 'HOURLY'
        elif interval_minutes == 1440:
            return 'DAILY'
        elif interval_minutes == 10080:
            return 'WEEKLY'
        else:
            return f'EVERY_{interval_minutes}_MINUTES'
    
    def create_inventory_dataframes(self) -> Tuple:
        """
        Create PySpark DataFrames from extracted inventories.
        
        Returns:
            Tuple of DataFrames (sources_df, targets_df, lineage_df, integrations_df)
        """
        logger.info("Creating inventory DataFrames")
        
        # Sources DataFrame
        sources_schema = StructType([
            StructField("source_id", StringType(), True),
            StructField("source_name", StringType(), True),
            StructField("database_type", StringType(), True),
            StructField("owner_name", StringType(), True),
            StructField("db_name", StringType(), True),
            StructField("source_type", StringType(), True),
            StructField("description", StringType(), True),
            StructField("field_count", IntegerType(), True),
            StructField("extraction_timestamp", StringType(), True)
        ])
        
        sources_data = [
            (
                s.get('source_id'),
                s.get('source_name'),
                s.get('database_type'),
                s.get('owner_name'),
                s.get('db_name'),
                s.get('source_type'),
                s.get('description'),
                s.get('field_count'),
                s.get('extraction_timestamp')
            )
            for s in self.sources_inventory
        ]
        
        sources_df = self.spark.createDataFrame(sources_data, sources_schema)
        
        # Targets DataFrame
        targets_schema = StructType([
            StructField("target_id", StringType(), True),
            StructField("target_name", StringType(), True),
            StructField("database_type", StringType(), True),
            StructField("owner_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("target_type", StringType(), True),
            StructField("description", StringType(), True),
            StructField("field_count", IntegerType(), True),
            StructField("extraction_timestamp", StringType(), True)
        ])
        
        targets_data = [
            (
                t.get('target_id'),
                t.get('target_name'),
                t.get('database_type'),
                t.get('owner_name'),
                t.get('table_name'),
                t.get('target_type'),
                t.get('description'),
                t.get('field_count'),
                t.get('extraction_timestamp')
            )
            for t in self.targets_inventory
        ]
        
        targets_df = self.spark.createDataFrame(targets_data, targets_schema)
        
        # Lineage DataFrame
        lineage_schema = StructType([
            StructField("mapping_name", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("source_field", StringType(), True),
            StructField("target_system", StringType(), True),
            StructField("target_field", StringType(), True),
            StructField("extraction_timestamp", StringType(), True)
        ])
        
        lineage_data = [
            (
                l.get('mapping_name'),
                l.get('source_system'),
                l.get('source_field'),
                l.get('target_system'),
                l.get('target_field'),
                l.get('extraction_timestamp')
            )
            for l in self.lineage_mappings
        ]
        
        lineage_df = self.spark.createDataFrame(lineage_data, lineage_schema)
        
        # Integrations DataFrame
        integrations_schema = StructType([
            StructField("integration_type", StringType(), True),
            StructField("name", StringType(), True),
            StructField("endpoint", StringType(), True),
            StructField("description", StringType(), True),
            StructField("extraction_timestamp", StringType(), True)
        ])
        
        integrations_data = [
            (
                i.get('integration_type'),
                i.get('name'),
                i.get('endpoint', i.get('queue_name', '')),
                i.get('description'),
                i.get('extraction_timestamp')
            )
            for i in self.integration_points
        ]
        
        integrations_df = self.spark.createDataFrame(integrations_data, integrations_schema)
        
        return sources_df, targets_df, lineage_df, integrations_df
    
    def generate_documentation(self, sources_df, targets_df, lineage_df, integrations_df):
        """
        Generate comprehensive documentation and save to output path.
        
        Args:
            sources_df: Sources DataFrame
            targets_df: Targets DataFrame
            lineage_df: Lineage DataFrame
            integrations_df: Integrations DataFrame
        """
        logger.info("Generating documentation")
        
        # Save DataFrames as Parquet
        sources_df.write.mode('overwrite').parquet(f"{self.output_path}/sources_inventory")
        targets_df.write.mode('overwrite').parquet(f"{self.output_path}/targets_inventory")
        lineage_df.write.mode('overwrite').parquet(f"{self.output_path}/lineage_mappings")
        integrations_df.write.mode('overwrite').parquet(f"{self.output_path}/integration_points")
        
        # Generate summary statistics
        sources_summary = sources_df.groupBy('database_type').agg(
            count('*').alias('source_count'),
            sum('field_count').alias('total_fields')
        )
        
        targets_summary = targets_df.groupBy('database_type').agg(
            count('*').alias('target_count'),
            sum('field_count').alias('total_fields')
        )
        
        # Save summaries
        sources_summary.write.mode('overwrite').parquet(f"{self.output_path}/sources_summary")
        targets_summary.write.mode('overwrite').parquet(f"{self.output_path}/targets_summary")
        
        # Create lineage graph data
        lineage_graph = lineage_df.select(
            col('source_system').alias('from'),
            col('target_system').alias('to'),
            col('mapping_name').alias('via')
        ).distinct()
        
        lineage_graph.write.mode('overwrite').parquet(f"{self.output_path}/lineage_graph")
        
        # Generate JSON documentation
        documentation = {
            'metadata': {
                'extraction_date': datetime.now().isoformat(),
                'total_sources': sources_df.count(),
                'total_targets': targets_df.count(),
                'total_lineage_mappings': lineage_df.count(),
                'total_integrations': integrations_df.count()
            },
            'sources_by_type': sources_summary.toPandas().to_dict('records'),
            'targets_by_type': targets_summary.toPandas().to_dict('records')
        }
        
        # Save JSON documentation
        json_path = f"{self.output_path}/documentation_summary.json"
        with open(json_path, 'w') as f:
            json.dump(documentation, f, indent=2)
        
        logger.info(f"Documentation generated successfully at {self.output_path}")
    
    def execute_full_extraction(self, xml_files: List[str]):
        """
        Execute full extraction process for all XML files.
        
        Args:
            xml_files: List of Informatica XML file paths
        """
        logger.info(f"Starting full extraction for {len(xml_files)} XML files")
        
        for xml_file in xml_files:
            logger.info(f"Processing file: {xml_file}")
            
            # Extract all components
            self.extract_source_connections(xml_file)
            self.extract_target_connections(xml_file)
            self.identify_file_sources(xml_file)
            self.extract_data_volumes_frequencies(xml_file)
            self.identify_api_web_services(xml_file)
            self.create_source_target_lineage(xml_file)
            self.extract_data_quality_rules(xml_file)
        
        # Create DataFrames
        sources_df, targets_df, lineage_df, integrations_df = self.create_inventory_dataframes()
        
        # Generate documentation
        self.generate_documentation(sources_df, targets_df, lineage_df, integrations_df)
        
        logger.info("Full extraction completed successfully")


def main():
    """
    Main execution function.
    """
    # Initialize Spark Session
    spark =