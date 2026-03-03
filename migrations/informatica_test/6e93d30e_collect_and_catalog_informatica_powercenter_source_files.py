import os
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from pyspark.sql.functions import col, lit, current_timestamp, explode, collect_list, struct, count, when
from datetime import datetime
import json
import hashlib
from typing import Dict, List, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InformaticaPowerCenterCollector:
    """
    Collects and catalogs Informatica PowerCenter artifacts for migration analysis.
    Processes XML exports, workflow definitions, mappings, sessions, and transformations.
    """
    
    def __init__(self, spark: SparkSession, base_path: str, output_path: str):
        """
        Initialize the collector with Spark session and paths.
        
        Args:
            spark: Active SparkSession
            base_path: Root path containing Informatica XML exports
            output_path: Path to write cataloged artifacts
        """
        self.spark = spark
        self.base_path = base_path
        self.output_path = output_path
        self.collection_timestamp = datetime.now()
        
    def collect_xml_files(self) -> List[str]:
        """
        Recursively collect all XML files from base path.
        
        Returns:
            List of full paths to XML files
        """
        logger.info(f"Collecting XML files from {self.base_path}")
        xml_files = []
        
        try:
            # Using Spark to read file system paths
            sc = self.spark.sparkContext
            hadoop_conf = sc._jsc.hadoopConfiguration()
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            path = sc._jvm.org.apache.hadoop.fs.Path(self.base_path)
            
            def traverse_directory(dir_path):
                file_status_list = fs.listStatus(dir_path)
                for file_status in file_status_list:
                    file_path = file_status.getPath()
                    if file_status.isDirectory():
                        traverse_directory(file_path)
                    elif str(file_path).upper().endswith('.XML'):
                        xml_files.append(str(file_path))
            
            traverse_directory(path)
            logger.info(f"Found {len(xml_files)} XML files")
            
        except Exception as e:
            logger.error(f"Error collecting XML files: {str(e)}")
            raise
        
        return xml_files
    
    def parse_workflow_xml(self, xml_path: str) -> Dict:
        """
        Parse workflow XML and extract metadata.
        
        Args:
            xml_path: Path to workflow XML file
            
        Returns:
            Dictionary containing workflow metadata
        """
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            workflows = []
            for workflow in root.findall('.//WORKFLOW'):
                wf_data = {
                    'workflow_name': workflow.get('NAME', ''),
                    'workflow_type': workflow.get('WORKFLOWTYPE', ''),
                    'version': workflow.get('VERSION', ''),
                    'is_valid': workflow.get('ISVALID', ''),
                    'description': workflow.get('DESCRIPTION', ''),
                    'server_name': workflow.get('SERVERNAME', ''),
                    'scheduler_info': workflow.get('SCHEDULERINFO', ''),
                    'tasks': [],
                    'source_file': xml_path,
                    'file_hash': self._calculate_file_hash(xml_path)
                }
                
                # Extract tasks
                for task in workflow.findall('.//TASK'):
                    task_data = {
                        'task_name': task.get('NAME', ''),
                        'task_type': task.get('TASKTYPE', ''),
                        'reusable': task.get('REUSABLE', ''),
                        'description': task.get('DESCRIPTION', '')
                    }
                    wf_data['tasks'].append(task_data)
                
                workflows.append(wf_data)
            
            return {'workflows': workflows, 'xml_path': xml_path}
            
        except Exception as e:
            logger.error(f"Error parsing workflow XML {xml_path}: {str(e)}")
            return {'workflows': [], 'xml_path': xml_path, 'error': str(e)}
    
    def parse_mapping_xml(self, xml_path: str) -> Dict:
        """
        Parse mapping XML and extract transformation logic.
        
        Args:
            xml_path: Path to mapping XML file
            
        Returns:
            Dictionary containing mapping metadata
        """
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            mappings = []
            for mapping in root.findall('.//MAPPING'):
                map_data = {
                    'mapping_name': mapping.get('NAME', ''),
                    'mapping_type': mapping.get('MAPPINGTYPE', ''),
                    'version': mapping.get('VERSION', ''),
                    'is_valid': mapping.get('ISVALID', ''),
                    'description': mapping.get('DESCRIPTION', ''),
                    'sources': [],
                    'targets': [],
                    'transformations': [],
                    'source_file': xml_path,
                    'file_hash': self._calculate_file_hash(xml_path)
                }
                
                # Extract sources
                for source in mapping.findall('.//SOURCE'):
                    source_data = {
                        'source_name': source.get('NAME', ''),
                        'database_type': source.get('DATABASETYPE', ''),
                        'dbdname': source.get('DBDNAME', ''),
                        'owner_name': source.get('OWNERNAME', ''),
                        'source_type': source.get('SOURCETYPE', ''),
                        'columns': []
                    }
                    
                    # Extract source columns
                    for field in source.findall('.//SOURCEFIELD'):
                        column_data = {
                            'name': field.get('NAME', ''),
                            'datatype': field.get('DATATYPE', ''),
                            'precision': field.get('PRECISION', ''),
                            'scale': field.get('SCALE', ''),
                            'nullable': field.get('NULLABLE', '')
                        }
                        source_data['columns'].append(column_data)
                    
                    map_data['sources'].append(source_data)
                
                # Extract targets
                for target in mapping.findall('.//TARGET'):
                    target_data = {
                        'target_name': target.get('NAME', ''),
                        'database_type': target.get('DATABASETYPE', ''),
                        'table_name': target.get('TABLENAME', ''),
                        'owner_name': target.get('OWNERNAME', ''),
                        'target_type': target.get('TARGETTYPE', ''),
                        'columns': []
                    }
                    
                    # Extract target columns
                    for field in target.findall('.//TARGETFIELD'):
                        column_data = {
                            'name': field.get('NAME', ''),
                            'datatype': field.get('DATATYPE', ''),
                            'precision': field.get('PRECISION', ''),
                            'scale': field.get('SCALE', ''),
                            'nullable': field.get('NULLABLE', ''),
                            'key_type': field.get('KEYTYPE', '')
                        }
                        target_data['columns'].append(column_data)
                    
                    map_data['targets'].append(target_data)
                
                # Extract transformations
                for transform_type in ['EXPRESSION', 'AGGREGATOR', 'JOINER', 'FILTER', 'LOOKUP', 
                                       'ROUTER', 'SORTER', 'UNION', 'UPDATE_STRATEGY', 'NORMALIZER',
                                       'RANK', 'SEQUENCE_GENERATOR', 'STORED_PROCEDURE']:
                    for transform in mapping.findall(f'.//{transform_type}'):
                        transform_data = {
                            'transformation_name': transform.get('NAME', ''),
                            'transformation_type': transform_type,
                            'description': transform.get('DESCRIPTION', ''),
                            'reusable': transform.get('REUSABLE', ''),
                            'expressions': [],
                            'ports': []
                        }
                        
                        # Extract transformation expressions
                        for expr in transform.findall('.//TRANSFORMFIELD'):
                            expr_data = {
                                'name': expr.get('NAME', ''),
                                'datatype': expr.get('DATATYPE', ''),
                                'precision': expr.get('PRECISION', ''),
                                'expression': expr.get('EXPRESSION', ''),
                                'port_type': expr.get('PORTTYPE', '')
                            }
                            transform_data['expressions'].append(expr_data)
                        
                        map_data['transformations'].append(transform_data)
                
                mappings.append(map_data)
            
            return {'mappings': mappings, 'xml_path': xml_path}
            
        except Exception as e:
            logger.error(f"Error parsing mapping XML {xml_path}: {str(e)}")
            return {'mappings': [], 'xml_path': xml_path, 'error': str(e)}
    
    def parse_session_xml(self, xml_path: str) -> Dict:
        """
        Parse session XML and extract session configurations.
        
        Args:
            xml_path: Path to session XML file
            
        Returns:
            Dictionary containing session metadata
        """
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            sessions = []
            for session in root.findall('.//SESSION'):
                session_data = {
                    'session_name': session.get('NAME', ''),
                    'mapping_name': session.get('MAPPINGNAME', ''),
                    'version': session.get('VERSION', ''),
                    'is_valid': session.get('ISVALID', ''),
                    'description': session.get('DESCRIPTION', ''),
                    'reusable': session.get('REUSABLE', ''),
                    'configurations': {},
                    'source_file': xml_path,
                    'file_hash': self._calculate_file_hash(xml_path)
                }
                
                # Extract session configuration attributes
                for config in session.findall('.//ATTRIBUTE'):
                    attr_name = config.get('NAME', '')
                    attr_value = config.get('VALUE', '')
                    session_data['configurations'][attr_name] = attr_value
                
                # Extract connection information
                session_data['connections'] = []
                for conn in session.findall('.//CONNECTION'):
                    conn_data = {
                        'name': conn.get('NAME', ''),
                        'connection_type': conn.get('CONNECTIONTYPE', ''),
                        'database_type': conn.get('DATABASETYPE', ''),
                        'username': conn.get('USERNAME', ''),
                        'connection_string': conn.get('CONNECTIONSTRING', '')
                    }
                    session_data['connections'].append(conn_data)
                
                sessions.append(session_data)
            
            return {'sessions': sessions, 'xml_path': xml_path}
            
        except Exception as e:
            logger.error(f"Error parsing session XML {xml_path}: {str(e)}")
            return {'sessions': [], 'xml_path': xml_path, 'error': str(e)}
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """
        Calculate MD5 hash of file for verification.
        
        Args:
            file_path: Path to file
            
        Returns:
            MD5 hash string
        """
        try:
            with open(file_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except:
            return ''
    
    def create_artifact_dataframes(self, xml_files: List[str]) -> Dict:
        """
        Parse all XML files and create DataFrames for each artifact type.
        
        Args:
            xml_files: List of XML file paths
            
        Returns:
            Dictionary of DataFrames by artifact type
        """
        logger.info("Parsing XML files and creating DataFrames")
        
        workflows_list = []
        mappings_list = []
        sessions_list = []
        transformations_list = []
        sources_list = []
        targets_list = []
        
        for xml_file in xml_files:
            try:
                # Determine file type and parse accordingly
                with open(xml_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read(1000)
                    
                if '<WORKFLOW' in content:
                    wf_data = self.parse_workflow_xml(xml_file)
                    workflows_list.extend(wf_data.get('workflows', []))
                
                if '<MAPPING' in content:
                    map_data = self.parse_mapping_xml(xml_file)
                    for mapping in map_data.get('mappings', []):
                        mappings_list.append({
                            'mapping_name': mapping['mapping_name'],
                            'mapping_type': mapping['mapping_type'],
                            'version': mapping['version'],
                            'is_valid': mapping['is_valid'],
                            'description': mapping['description'],
                            'source_file': mapping['source_file'],
                            'file_hash': mapping['file_hash'],
                            'source_count': len(mapping['sources']),
                            'target_count': len(mapping['targets']),
                            'transformation_count': len(mapping['transformations'])
                        })
                        
                        # Collect sources
                        for source in mapping['sources']:
                            source_entry = source.copy()
                            source_entry['mapping_name'] = mapping['mapping_name']
                            source_entry['column_count'] = len(source['columns'])
                            source_entry['columns_json'] = json.dumps(source['columns'])
                            sources_list.append(source_entry)
                        
                        # Collect targets
                        for target in mapping['targets']:
                            target_entry = target.copy()
                            target_entry['mapping_name'] = mapping['mapping_name']
                            target_entry['column_count'] = len(target['columns'])
                            target_entry['columns_json'] = json.dumps(target['columns'])
                            targets_list.append(target_entry)
                        
                        # Collect transformations
                        for transform in mapping['transformations']:
                            transform_entry = {
                                'mapping_name': mapping['mapping_name'],
                                'transformation_name': transform['transformation_name'],
                                'transformation_type': transform['transformation_type'],
                                'description': transform['description'],
                                'reusable': transform['reusable'],
                                'expression_count': len(transform['expressions']),
                                'expressions_json': json.dumps(transform['expressions'])
                            }
                            transformations_list.append(transform_entry)
                
                if '<SESSION' in content:
                    sess_data = self.parse_session_xml(xml_file)
                    for session in sess_data.get('sessions', []):
                        session_entry = {
                            'session_name': session['session_name'],
                            'mapping_name': session['mapping_name'],
                            'version': session['version'],
                            'is_valid': session['is_valid'],
                            'description': session['description'],
                            'reusable': session['reusable'],
                            'source_file': session['source_file'],
                            'file_hash': session['file_hash'],
                            'configurations_json': json.dumps(session['configurations']),
                            'connections_json': json.dumps(session['connections'])
                        }
                        sessions_list.append(session_entry)
                        
            except Exception as e:
                logger.error(f"Error processing file {xml_file}: {str(e)}")
                continue
        
        # Create DataFrames
        workflows_df = self.spark.createDataFrame(workflows_list) if workflows_list else None
        mappings_df = self.spark.createDataFrame(mappings_list) if mappings_list else None
        sessions_df = self.spark.createDataFrame(sessions_list) if sessions_list else None
        transformations_df = self.spark.createDataFrame(transformations_list) if transformations_list else None
        sources_df = self.spark.createDataFrame(sources_list) if sources_list else None
        targets_df = self.spark.createDataFrame(targets_list) if targets_list else None
        
        return {
            'workflows': workflows_df,
            'mappings': mappings_df,
            'sessions': sessions_df,
            'transformations': transformations_df,
            'sources': sources_df,
            'targets': targets_df
        }
    
    def create_inventory_summary(self, artifact_dfs: Dict) -> None:
        """
        Create comprehensive inventory summary with counts and descriptions.
        
        Args:
            artifact_dfs: Dictionary of artifact DataFrames
        """
        logger.info("Creating inventory summary")
        
        summary_data = []
        
        for artifact_type, df in artifact_dfs.items():
            if df is not None:
                count = df.count()
                
                summary_entry = {
                    'artifact_type': artifact_type,
                    'total_count': count,
                    'collection_timestamp': self.collection_timestamp,
                    'source_path': self.base_path
                }
                
                # Add type-specific metrics
                if artifact_type == 'workflows':
                    summary_entry['unique_workflows'] = df.select('workflow_name').distinct().count()
                    summary_entry['task_count'] = df.selectExpr('size(tasks)').agg({'size(tasks)': 'sum'}).collect()[0][0] or 0
                
                elif artifact_type == 'mappings':
                    summary_entry['unique_mappings'] = df.select('mapping_name').distinct().count()
                    summary_entry['total_sources'] = df.agg({'source_count': 'sum'}).collect()[0][0] or 0
                    summary_entry['total_targets'] = df.agg({'target_count': 'sum'}).collect()[0][0] or 0
                    summary_entry['total_transformations'] = df.agg({'transformation_count': 'sum'}).collect()[0][0] or 0
                
                elif artifact_type == 'sessions':
                    summary_entry['unique_sessions'] = df.select('session_name').distinct().count()
                    summary_entry['reusable_count'] = df.filter(col('reusable') == 'YES').count()
                
                elif artifact_type == 'transformations':
                    summary_entry['unique_transformations'] = df.select('transformation_name').distinct().count()
                    # Count by transformation type
                    type_counts = df.groupBy('transformation_type').count().collect()
                    summary_entry['transformation_types'] = {row['transformation_type']: row['count'] for row in type_counts}
                
                elif artifact_type == 'sources':
                    summary_entry['unique_sources'] = df.select('source_name').distinct().count()
                    summary_entry['total_columns'] = df.agg({'column_count': 'sum'}).collect()[0][0] or 0
                
                elif artifact_type == 'targets':
                    summary_entry['unique_targets'] = df.select('target_name').distinct().count()
                    summary_entry['total_columns'] = df.agg({'column_count': 'sum'}).collect()[0][0] or 0
                
                summary_data.append(summary_entry)
        
        # Create summary DataFrame
        summary_df = self.spark.createDataFrame(summary_data)
        
        # Write summary to output
        summary_output = f"{self.output_path}/inventory_summary"
        summary_df.coalesce(1).write.mode('overwrite').json(summary_output)
        logger.info(f"Inventory summary written to {summary_output}")
        
        # Also create human-readable CSV
        summary_csv = f"{self.output_path}/inventory_summary.csv"
        summary_df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(summary_csv)
        logger.info(f"Inventory summary CSV written to {summary_csv}")
    
    def create_repository_structure(self, xml_files: List[str]) -> None:
        """
        Document repository folder structure and organization.
        
        Args:
            xml_files: List of XML file paths
        """
        logger.info("Creating repository structure documentation")
        
        structure_data = []
        
        for xml_file in xml_files:
            path_parts = xml_file.replace(self.base_path, '').split('/')
            
            structure_entry = {
                'full_path': xml_file,
                'relative_path': xml_file.replace(self.base_path, ''),
                'folder_depth': len(path_parts) - 1,
                'folder_name': path_parts[-2] if len(path_parts) > 1 else 'root',
                'file_name': path_parts[-1],
                'file_size_bytes': os.path.getsize(xml_file) if os.path.exists(xml_file) else 0,
                'collection_timestamp': self.collection_timestamp
            }
            
            structure_data.append(structure_entry)
        
        structure_df = self.spark.createDataFrame(structure_data)
        
        # Write structure to output
        structure_output = f"{self.output_path}/repository_structure"
        structure_df.write.mode('overwrite').parquet(structure_output)
        logger.info(f"Repository structure written to {structure_output}")
        
        # Create folder hierarchy summary
        folder_summary = structure_df.groupBy('folder_name').agg(
            count('*').alias('file_count'),
            sum('file_size_bytes').alias('total_size_bytes')
        )
        
        folder_summary_output = f"{self.output_path}/folder_hierarchy_summary"
        folder_summary.coalesce(1).write.mode('overwrite').option('header', 'true').csv(folder_summary_output)
        logger.info(f"Folder hierarchy summary written to {folder_summary_output}")
    
    def create_source_to_target_mappings(self, artifact_dfs: Dict) -> None:
        """
        Create comprehensive source-to-target mapping documentation.
        
        Args:
            artifact_dfs: Dictionary of artifact DataFrames
        """
        logger.info("Creating source-to-target mapping documentation")
        
        if artifact_dfs.get('mappings') and artifact_dfs.get('sources') and artifact_dfs.get('targets'):
            # Join mappings with sources and targets
            s2t_mapping = artifact_dfs['sources'].alias('src') \
                .join(
                    artifact_dfs['targets'].alias('tgt'),
                    col('src.mapping_name') == col('tgt.mapping_name'),
                    'inner'
                ) \
                .select(
                    col('src.mapping_name').alias('mapping_name'),
                    col('src.source_name').alias('source_name'),
                    col('src.database_type').alias('source_database_type'),
                    col('src.dbdname').alias('source_dbdname'),
                    col('src.source_type').alias('source_type'),
                    col('src.column_count').alias('source_column_count'),
                    col('tgt.target_name').alias('target_name'),
                    col('tgt.database_type').alias('target_database_type'),
                    col('tgt.table_name').alias('target_table_name'),
                    col('tgt.target_type').alias('target_type'),
                    col('tgt.column_count').alias('target_column_count'),
                    lit(self.collection_timestamp).alias('collection_timestamp')
                )
            
            # Write source-to-target mappings
            s2t_output = f"{self.output_path}/source_to_target_mappings"
            s2t_mapping.write.mode('overwrite').parquet(s2t_output)
            logger.info(f"Source-to-target mappings written to {s2t_output}")
            
            # Create CSV version for easy viewing
            s2t_csv = f"{self.output_path}/source_to_target_mappings.csv"
            s2t_mapping.coalesce(1).write.mode('overwrite').option('header', 'true').csv(s2t_csv)
            logger.info(f"Source-to-target mappings CSV written to {s2t_csv}")
    
    def save_artifacts(self, artifact_dfs: Dict) -> None:
        """
        Save all artifact DataFrames to output location.
        
        Args:
            artifact_dfs: Dictionary of artifact DataFrames
        """
        logger.info("Saving artifact DataFrames")
        
        for artifact_type, df in artifact_dfs.items():
            if df is not None:
                output_path = f"{self.output_path}/artifacts/{artifact_type}"
                df.write.mode('overwrite').parquet(output_path)
                logger.info(f"Saved {artifact_type} to {output_path}")
                
                # Also save as CSV for easy viewing
                csv_path = f"{self.output_path}/artifacts_csv/{artifact_type}.csv"
                df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(csv_path)
                logger.info(f"Saved {artifact_type} CSV to {csv_path}")
    
    def run_collection(self) -> None:
        """
        Execute the complete collection and cataloging process.
        """
        logger.info("Starting Informatica PowerCenter artifact collection")
        
        try:
            # Step 1: Collect all XML files
            xml_files = self.collect_xml_files()
            
            if not xml_files:
                logger.warning("No XML files found. Exiting.")
                return
            
            # Step 2: Parse XML files and create DataFrames
            artifact_dfs = self.create_artifact_dataframes(xml_files)
            
            # Step 3: Save artifacts
            self.save_artifacts(artifact_dfs)
            
            # Step 4: Create inventory summary
            self.create_inventory_summary(artifact_dfs)
            
            # Step 5: Document repository structure
            self.create_repository_structure(xml_files)
            
            # Step 6: Create source-to-target mappings
            self.create_source_to_target_mappings(artifact_dfs)
            
            logger.info("Informatica PowerCenter artifact collection completed successfully")
            
        except Exception as e:
            logger.error(f"Error during collection process: {str(e)}")
            raise


# Main execution
if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("InformaticaPowerCenterCollector") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Configuration parameters
    BASE_PATH = "/path/to/informatica/exports"
    OUTPUT_PATH = "/path/to/migration/catalog"
    
    # Initialize collector
    collector = InformaticaPowerCenterCollector(
        spark=spark,
        base_path=BASE_PATH,
        output_path=OUTPUT_PATH
    )
    
    # Run collection process
    collector.run_collection()
    
    # Stop Spark session
    spark.stop()