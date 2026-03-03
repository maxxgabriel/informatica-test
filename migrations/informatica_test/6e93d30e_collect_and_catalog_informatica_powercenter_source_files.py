import os
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Any, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, count, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class InformaticaArtifactCollector:
    """
    Collects and catalogs Informatica PowerCenter artifacts for migration analysis.
    Processes XML exports, workflow definitions, mappings, sessions, and transformations.
    """
    
    def __init__(self, spark: SparkSession, source_path: str, output_path: str):
        """
        Initialize the artifact collector.
        
        Args:
            spark: Active SparkSession
            source_path: Root path containing Informatica XML exports
            output_path: Path to store processed inventory and metadata
        """
        self.spark = spark
        self.source_path = source_path
        self.output_path = output_path
        self.inventory_data = {
            'workflows': [],
            'mappings': [],
            'sessions': [],
            'transformations': [],
            'parameter_files': [],
            'repository_structure': []
        }
        
    def parse_xml_file(self, file_path: str) -> ET.Element:
        """
        Parse XML file and return root element.
        
        Args:
            file_path: Path to XML file
            
        Returns:
            XML root element
        """
        try:
            tree = ET.parse(file_path)
            return tree.getroot()
        except ET.ParseError as e:
            logger.error(f"Error parsing XML file {file_path}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing {file_path}: {str(e)}")
            return None
    
    def extract_workflow_metadata(self, root: ET.Element, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract workflow metadata from XML.
        
        Args:
            root: XML root element
            file_path: Source file path
            
        Returns:
            List of workflow metadata dictionaries
        """
        workflows = []
        
        for workflow in root.findall('.//WORKFLOW'):
            wf_data = {
                'workflow_name': workflow.get('NAME', ''),
                'workflow_type': workflow.get('WORKFLOW_TYPE', 'STANDARD'),
                'description': workflow.get('DESCRIPTION', ''),
                'is_valid': workflow.get('ISVALID', 'YES'),
                'version_number': workflow.get('VERSION_NUMBER', '1'),
                'source_file': os.path.basename(file_path),
                'file_path': file_path,
                'tasks': [],
                'dependencies': [],
                'scheduler_info': {},
                'extracted_timestamp': datetime.now().isoformat()
            }
            
            for task in workflow.findall('.//TASK'):
                task_data = {
                    'task_name': task.get('NAME', ''),
                    'task_type': task.get('TYPE', ''),
                    'reusable': task.get('REUSABLE', 'NO'),
                    'description': task.get('DESCRIPTION', '')
                }
                wf_data['tasks'].append(task_data)
            
            for link in workflow.findall('.//TASKLINK'):
                link_data = {
                    'from_task': link.get('FROMTASK', ''),
                    'to_task': link.get('TOTASK', ''),
                    'condition': link.get('CONDITION', '')
                }
                wf_data['dependencies'].append(link_data)
            
            scheduler = workflow.find('.//SCHEDULER')
            if scheduler is not None:
                wf_data['scheduler_info'] = {
                    'scheduler_type': scheduler.get('SCHEDULETYPE', ''),
                    'start_time': scheduler.get('STARTTIME', ''),
                    'end_time': scheduler.get('ENDTIME', ''),
                    'repeat_interval': scheduler.get('REPEATINTERVAL', '')
                }
            
            workflows.append(wf_data)
        
        return workflows
    
    def extract_mapping_metadata(self, root: ET.Element, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract mapping metadata from XML.
        
        Args:
            root: XML root element
            file_path: Source file path
            
        Returns:
            List of mapping metadata dictionaries
        """
        mappings = []
        
        for mapping in root.findall('.//MAPPING'):
            mapping_data = {
                'mapping_name': mapping.get('NAME', ''),
                'mapping_type': mapping.get('MAPPINGTYPE', 'Standard'),
                'description': mapping.get('DESCRIPTION', ''),
                'is_valid': mapping.get('ISVALID', 'YES'),
                'version_number': mapping.get('VERSION_NUMBER', '1'),
                'source_file': os.path.basename(file_path),
                'file_path': file_path,
                'sources': [],
                'targets': [],
                'transformations': [],
                'mapplets': [],
                'extracted_timestamp': datetime.now().isoformat()
            }
            
            for source in mapping.findall('.//SOURCE'):
                source_data = {
                    'source_name': source.get('NAME', ''),
                    'source_type': source.get('SOURCETYPE', ''),
                    'database_type': source.get('DATABASETYPE', ''),
                    'owner_name': source.get('OWNERNAME', ''),
                    'table_name': source.get('NAME', ''),
                    'columns': []
                }
                
                for field in source.findall('.//SOURCEFIELD'):
                    field_data = {
                        'field_name': field.get('NAME', ''),
                        'datatype': field.get('DATATYPE', ''),
                        'precision': field.get('PRECISION', ''),
                        'scale': field.get('SCALE', ''),
                        'nullable': field.get('NULLABLE', '')
                    }
                    source_data['columns'].append(field_data)
                
                mapping_data['sources'].append(source_data)
            
            for target in mapping.findall('.//TARGET'):
                target_data = {
                    'target_name': target.get('NAME', ''),
                    'target_type': target.get('TARGETTYPE', ''),
                    'database_type': target.get('DATABASETYPE', ''),
                    'table_name': target.get('NAME', ''),
                    'load_type': target.get('LOADTYPE', 'NORMAL'),
                    'columns': []
                }
                
                for field in target.findall('.//TARGETFIELD'):
                    field_data = {
                        'field_name': field.get('NAME', ''),
                        'datatype': field.get('DATATYPE', ''),
                        'precision': field.get('PRECISION', ''),
                        'scale': field.get('SCALE', ''),
                        'key_type': field.get('KEYTYPE', '')
                    }
                    target_data['columns'].append(field_data)
                
                mapping_data['targets'].append(target_data)
            
            for transform in mapping.findall('.//TRANSFORMATION'):
                transform_data = self.extract_transformation_details(transform)
                mapping_data['transformations'].append(transform_data)
            
            mappings.append(mapping_data)
        
        return mappings
    
    def extract_transformation_details(self, transform: ET.Element) -> Dict[str, Any]:
        """
        Extract detailed transformation metadata.
        
        Args:
            transform: Transformation XML element
            
        Returns:
            Transformation metadata dictionary
        """
        transform_data = {
            'transformation_name': transform.get('NAME', ''),
            'transformation_type': transform.get('TYPE', ''),
            'description': transform.get('DESCRIPTION', ''),
            'reusable': transform.get('REUSABLE', 'NO'),
            'properties': {},
            'ports': [],
            'expressions': [],
            'group_by_fields': [],
            'join_conditions': [],
            'filter_conditions': []
        }
        
        transform_type = transform.get('TYPE', '')
        
        for prop in transform.findall('.//TABLEATTRIBUTE'):
            prop_name = prop.get('NAME', '')
            prop_value = prop.get('VALUE', '')
            transform_data['properties'][prop_name] = prop_value
        
        for port in transform.findall('.//TRANSFORMFIELD'):
            port_data = {
                'port_name': port.get('NAME', ''),
                'datatype': port.get('DATATYPE', ''),
                'precision': port.get('PRECISION', ''),
                'scale': port.get('SCALE', ''),
                'port_type': port.get('PORTTYPE', ''),
                'expression': port.get('EXPRESSION', ''),
                'default_value': port.get('DEFAULTVALUE', '')
            }
            transform_data['ports'].append(port_data)
            
            if port.get('EXPRESSION'):
                transform_data['expressions'].append({
                    'field': port.get('NAME', ''),
                    'expression': port.get('EXPRESSION', '')
                })
        
        if transform_type == 'Aggregator':
            for port in transform.findall('.//TRANSFORMFIELD[@PORTTYPE="INPUT/OUTPUT"]'):
                if port.get('GROUPBY') == 'YES':
                    transform_data['group_by_fields'].append(port.get('NAME', ''))
        
        elif transform_type == 'Joiner':
            join_condition = transform.find('.//TABLEATTRIBUTE[@NAME="Join Condition"]')
            if join_condition is not None:
                transform_data['join_conditions'].append(join_condition.get('VALUE', ''))
            
            join_type = transform.find('.//TABLEATTRIBUTE[@NAME="Join Type"]')
            if join_type is not None:
                transform_data['properties']['join_type'] = join_type.get('VALUE', '')
        
        elif transform_type == 'Filter':
            filter_condition = transform.find('.//TABLEATTRIBUTE[@NAME="Filter Condition"]')
            if filter_condition is not None:
                transform_data['filter_conditions'].append(filter_condition.get('VALUE', ''))
        
        return transform_data
    
    def extract_session_metadata(self, root: ET.Element, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract session metadata from XML.
        
        Args:
            root: XML root element
            file_path: Source file path
            
        Returns:
            List of session metadata dictionaries
        """
        sessions = []
        
        for session in root.findall('.//SESSION'):
            session_data = {
                'session_name': session.get('NAME', ''),
                'mapping_name': session.get('MAPPINGNAME', ''),
                'description': session.get('DESCRIPTION', ''),
                'is_valid': session.get('ISVALID', 'YES'),
                'source_file': os.path.basename(file_path),
                'file_path': file_path,
                'config_properties': {},
                'connection_info': {},
                'session_properties': {},
                'extracted_timestamp': datetime.now().isoformat()
            }
            
            for config in session.findall('.//CONFIGREFERENCE'):
                session_data['config_properties'][config.get('TYPE', '')] = {
                    'ref_name': config.get('REFOBJECTNAME', ''),
                    'type': config.get('TYPE', '')
                }
            
            for attribute in session.findall('.//ATTRIBUTE'):
                attr_name = attribute.get('NAME', '')
                attr_value = attribute.get('VALUE', '')
                session_data['session_properties'][attr_name] = attr_value
            
            for sesstransformationinst in session.findall('.//SESSTRANSFORMATIONINST'):
                transform_name = sesstransformationinst.get('TRANSFORMATIONNAME', '')
                partition_type = sesstransformationinst.get('PARTITIONTYPE', '')
                
                if transform_name:
                    session_data['session_properties'][f'{transform_name}_partition'] = partition_type
            
            sessions.append(session_data)
        
        return sessions
    
    def extract_repository_structure(self, root: ET.Element, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract repository folder structure.
        
        Args:
            root: XML root element
            file_path: Source file path
            
        Returns:
            List of folder metadata dictionaries
        """
        folders = []
        
        for folder in root.findall('.//FOLDER'):
            folder_data = {
                'folder_name': folder.get('NAME', ''),
                'description': folder.get('DESCRIPTION', ''),
                'owner': folder.get('OWNER', ''),
                'group': folder.get('GROUP', ''),
                'permissions': folder.get('PERMISSIONS', ''),
                'source_file': os.path.basename(file_path),
                'file_path': file_path,
                'extracted_timestamp': datetime.now().isoformat()
            }
            folders.append(folder_data)
        
        return folders
    
    def scan_directory_structure(self, root_path: str) -> List[Dict[str, Any]]:
        """
        Scan and document directory structure.
        
        Args:
            root_path: Root directory path
            
        Returns:
            List of directory metadata
        """
        directory_structure = []
        
        for root, dirs, files in os.walk(root_path):
            for file in files:
                file_path = os.path.join(root, file)
                file_extension = os.path.splitext(file)[1].lower()
                
                file_info = {
                    'file_name': file,
                    'file_path': file_path,
                    'relative_path': os.path.relpath(file_path, root_path),
                    'file_extension': file_extension,
                    'file_size': os.path.getsize(file_path),
                    'file_type': self.determine_artifact_type(file_extension, file),
                    'directory': os.path.dirname(file_path),
                    'scanned_timestamp': datetime.now().isoformat()
                }
                directory_structure.append(file_info)
        
        return directory_structure
    
    def determine_artifact_type(self, extension: str, filename: str) -> str:
        """
        Determine artifact type based on file extension and name.
        
        Args:
            extension: File extension
            filename: File name
            
        Returns:
            Artifact type string
        """
        filename_lower = filename.lower()
        
        if extension == '.xml':
            if 'workflow' in filename_lower or filename_lower.startswith('wf_'):
                return 'WORKFLOW'
            elif 'mapping' in filename_lower or filename_lower.startswith('m_'):
                return 'MAPPING'
            elif 'session' in filename_lower or filename_lower.startswith('s_'):
                return 'SESSION'
            elif 'transformation' in filename_lower or filename_lower.startswith('t_'):
                return 'TRANSFORMATION'
            else:
                return 'XML_EXPORT'
        elif extension in ['.param', '.txt', '.properties']:
            return 'PARAMETER_FILE'
        elif extension == '.log':
            return 'SESSION_LOG'
        elif extension == '.json':
            return 'METADATA_JSON'
        else:
            return 'OTHER'
    
    def process_all_artifacts(self):
        """
        Process all Informatica artifacts in the source directory.
        """
        logger.info(f"Starting artifact collection from {self.source_path}")
        
        directory_structure = self.scan_directory_structure(self.source_path)
        logger.info(f"Found {len(directory_structure)} files")
        
        xml_files = [f for f in directory_structure if f['file_extension'] == '.xml']
        logger.info(f"Processing {len(xml_files)} XML files")
        
        for file_info in xml_files:
            file_path = file_info['file_path']
            logger.info(f"Processing: {file_path}")
            
            root = self.parse_xml_file(file_path)
            if root is None:
                continue
            
            workflows = self.extract_workflow_metadata(root, file_path)
            self.inventory_data['workflows'].extend(workflows)
            
            mappings = self.extract_mapping_metadata(root, file_path)
            self.inventory_data['mappings'].extend(mappings)
            
            sessions = self.extract_session_metadata(root, file_path)
            self.inventory_data['sessions'].extend(sessions)
            
            folders = self.extract_repository_structure(root, file_path)
            self.inventory_data['repository_structure'].extend(folders)
            
            for mapping in mappings:
                self.inventory_data['transformations'].extend(mapping['transformations'])
        
        param_files = [f for f in directory_structure if f['file_type'] == 'PARAMETER_FILE']
        self.inventory_data['parameter_files'] = param_files
        
        logger.info("Artifact collection completed")
    
    def create_inventory_dataframes(self) -> Dict[str, DataFrame]:
        """
        Create Spark DataFrames from collected inventory data.
        
        Returns:
            Dictionary of DataFrames for each artifact type
        """
        dataframes = {}
        
        if self.inventory_data['workflows']:
            workflows_df = self.spark.createDataFrame(
                [self.flatten_workflow(wf) for wf in self.inventory_data['workflows']]
            )
            dataframes['workflows'] = workflows_df
        
        if self.inventory_data['mappings']:
            mappings_df = self.spark.createDataFrame(
                [self.flatten_mapping(m) for m in self.inventory_data['mappings']]
            )
            dataframes['mappings'] = mappings_df
        
        if self.inventory_data['sessions']:
            sessions_df = self.spark.createDataFrame(self.inventory_data['sessions'])
            dataframes['sessions'] = sessions_df
        
        if self.inventory_data['transformations']:
            transformations_df = self.spark.createDataFrame(
                [self.flatten_transformation(t) for t in self.inventory_data['transformations']]
            )
            dataframes['transformations'] = transformations_df
        
        if self.inventory_data['parameter_files']:
            param_files_df = self.spark.createDataFrame(self.inventory_data['parameter_files'])
            dataframes['parameter_files'] = param_files_df
        
        if self.inventory_data['repository_structure']:
            repo_structure_df = self.spark.createDataFrame(self.inventory_data['repository_structure'])
            dataframes['repository_structure'] = repo_structure_df
        
        return dataframes
    
    def flatten_workflow(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten workflow dictionary for DataFrame creation.
        
        Args:
            workflow: Workflow metadata dictionary
            
        Returns:
            Flattened dictionary
        """
        return {
            'workflow_name': workflow['workflow_name'],
            'workflow_type': workflow['workflow_type'],
            'description': workflow['description'],
            'is_valid': workflow['is_valid'],
            'version_number': workflow['version_number'],
            'source_file': workflow['source_file'],
            'file_path': workflow['file_path'],
            'task_count': len(workflow['tasks']),
            'dependency_count': len(workflow['dependencies']),
            'has_scheduler': bool(workflow['scheduler_info']),
            'tasks_json': json.dumps(workflow['tasks']),
            'dependencies_json': json.dumps(workflow['dependencies']),
            'scheduler_json': json.dumps(workflow['scheduler_info']),
            'extracted_timestamp': workflow['extracted_timestamp']
        }
    
    def flatten_mapping(self, mapping: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten mapping dictionary for DataFrame creation.
        
        Args:
            mapping: Mapping metadata dictionary
            
        Returns:
            Flattened dictionary
        """
        return {
            'mapping_name': mapping['mapping_name'],
            'mapping_type': mapping['mapping_type'],
            'description': mapping['description'],
            'is_valid': mapping['is_valid'],
            'version_number': mapping['version_number'],
            'source_file': mapping['source_file'],
            'file_path': mapping['file_path'],
            'source_count': len(mapping['sources']),
            'target_count': len(mapping['targets']),
            'transformation_count': len(mapping['transformations']),
            'sources_json': json.dumps(mapping['sources']),
            'targets_json': json.dumps(mapping['targets']),
            'transformations_json': json.dumps(mapping['transformations']),
            'extracted_timestamp': mapping['extracted_timestamp']
        }
    
    def flatten_transformation(self, transformation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten transformation dictionary for DataFrame creation.
        
        Args:
            transformation: Transformation metadata dictionary
            
        Returns:
            Flattened dictionary
        """
        return {
            'transformation_name': transformation['transformation_name'],
            'transformation_type': transformation['transformation_type'],
            'description': transformation['description'],
            'reusable': transformation['reusable'],
            'port_count': len(transformation['ports']),
            'expression_count': len(transformation['expressions']),
            'properties_json': json.dumps(transformation['properties']),
            'ports_json': json.dumps(transformation['ports']),
            'expressions_json': json.dumps(transformation['expressions']),
            'group_by_fields_json': json.dumps(transformation['group_by_fields']),
            'join_conditions_json': json.dumps(transformation['join_conditions']),
            'filter_conditions_json': json.dumps(transformation['filter_conditions'])
        }
    
    def generate_summary_report(self, dataframes: Dict[str, DataFrame]) -> DataFrame:
        """
        Generate summary statistics report.
        
        Args:
            dataframes: Dictionary of artifact DataFrames
            
        Returns:
            Summary DataFrame
        """
        summary_data = []
        
        for artifact_type, df in dataframes.items():
            record_count = df.count()
            
            summary_data.append({
                'artifact_type': artifact_type,
                'total_count': record_count,
                'report_timestamp': datetime.now().isoformat()
            })
        
        summary_df = self.spark.createDataFrame(summary_data)
        return summary_df
    
    def generate_source_to_target_mapping(self, dataframes: Dict[str, DataFrame]) -> DataFrame:
        """
        Generate source-to-target mapping report.
        
        Args:
            dataframes: Dictionary of artifact DataFrames
            
        Returns:
            Source-to-target mapping DataFrame
        """
        if 'mappings' not in dataframes:
            logger.warning("No mappings found for source-to-target analysis")
            return None
        
        mappings_df = dataframes['mappings']
        
        from pyspark.sql.functions import explode, from_json
        from pyspark.sql.types import ArrayType, StructType, StructField, StringType
        
        source_schema = ArrayType(StructType([
            StructField("source_name", StringType(), True),
            StructField("source_type", StringType(), True),
            StructField("database_type", StringType(), True),
            StructField("table_name", StringType(), True)
        ]))
        
        target_schema = ArrayType(StructType([
            StructField("target_name", StringType(), True),
            StructField("target_type", StringType(), True),
            StructField("database_type", StringType(), True),
            StructField("table_name", StringType(), True)
        ]))
        
        mappings_with_sources = mappings_df.withColumn(
            "sources_parsed", from_json(col("sources_json"), source_schema)
        ).withColumn(
            "targets_parsed", from_json(col("targets_json"), target_schema)
        )
        
        source_target_df = mappings_with_sources.select(
            col("mapping_name"),
            col("mapping_type"),
            explode(col("sources_parsed")).alias("source"),
            explode(col("targets_parsed")).alias("target")
        ).select(
            col("mapping_name"),
            col("mapping_type"),
            col("source.source_name").alias("source_name"),
            col("source.table_name").alias("source_table"),
            col("source.database_type").alias("source_db_type"),
            col("target.target_name").alias("target_name"),
            col("target.table_name").alias("target_table"),
            col("target.database_type").alias("target_db_type")
        ).withColumn("created_timestamp", current_timestamp())
        
        return source_target_df
    
    def generate_transformation_report(self, dataframes: Dict[str, DataFrame]) -> DataFrame:
        """
        Generate transformation analysis report.
        
        Args:
            dataframes: Dictionary of artifact DataFrames
            
        Returns:
            Transformation analysis DataFrame
        """
        if 'transformations' not in dataframes:
            logger.warning("No transformations found for analysis")
            return None
        
        transformations_df = dataframes['transformations']
        
        transformation_summary = transformations_df.groupBy("transformation_type").agg(
            count("*").alias("count"),
            countDistinct("transformation_name").alias("unique_transformations")
        ).withColumn("analysis_timestamp", current_timestamp())
        
        return transformation_summary
    
    def save_inventory_to_storage(self, dataframes: Dict[str, DataFrame]):
        """
        Save all inventory DataFrames to storage.
        
        Args:
            dataframes: Dictionary of artifact DataFrames
        """
        logger.info(f"Saving inventory data to {self.output_path}")
        
        for artifact_type, df in dataframes.items():
            output_location = f"{self.output_path}/inventory/{artifact_type}"
            
            df.coalesce(1).write.mode("overwrite").parquet(output_location)
            logger.info(f"Saved {artifact_type} to {output_location}")
            
            csv_location = f"{self.output_path}/inventory_csv/{artifact_type}"
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_location)
            logger.info(f"Saved {artifact_type} CSV to {csv_location}")
        
        summary_df = self.generate_summary_report(dataframes)
        summary_location = f"{self.output_path}/summary/inventory_summary"
        summary_df.write.mode("overwrite").parquet(summary_location)
        summary_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            f"{self.output_path}/summary_csv/inventory_summary"
        )
        
        source_target_df = self.generate_source_to_target_mapping(dataframes)
        if source_target_df:
            st_location = f"{self.output_path}/analysis/source_to_target_mapping"
            source_target_df.write.mode("overwrite").parquet(st_location)
            source_target_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
                f"{self.output_path}/analysis_csv/source_to_target_mapping"
            )
        
        transformation_report = self.generate_transformation_report(dataframes)
        if transformation_report:
            trans_location = f"{self.output_path}/analysis/transformation_summary"
            transformation_report.write.mode("overwrite").parquet(trans_location)
            transformation_report.coalesce(1).write.mode("overwrite").option("header", "true").csv(
                f"{self.output_path}/analysis_csv/transformation_summary"
            )
        
        metadata_output = f"{self.output_path}/metadata/inventory_metadata.json"
        os.makedirs(os.path.dirname(metadata_output), exist_ok=True)
        with open(metadata_output, 'w') as f:
            json.dump(self.inventory_data, f, indent=2)
        logger.info(f"Saved metadata JSON to {metadata_output}")
    
    def execute_collection(self):
        """
        Execute complete artifact collection and cataloging process.
        """
        try:
            logger.info("Starting Informatica artifact collection process")
            
            self.process_all_artifacts()
            
            dataframes = self.create_inventory_dataframes()
            
            self.save_inventory_to_storage(dataframes)
            
            logger.info("Informatica artifact collection completed successfully")
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Error during artifact collection: {str(e)}")
            raise


def main():
    """
    Main execution function for Informatica artifact collection.
    """
    spark = SparkSession.builder \
        .appName("InformaticaArtifactCollector") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    source_path = "/path/to/informatica/exports"
    output_path = "/path/to/output/inventory"
    
    collector = InformaticaArtifactCollector(
        spark=spark,
        source_path=source_path,
        output_path=output_path
    )
    
    dataframes = collector.execute_collection()
    
    for artifact_type, df in dataframes.items():
        print(f"\n{artifact_type.upper()} Summary:")
        df.show(10, truncate=False)
    
    spark.stop()


if __name__ == "__main__":
    main()