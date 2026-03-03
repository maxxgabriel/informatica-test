import xml.etree.ElementTree as ET
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import os
import re
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import yaml

class InformaticaMetadataExtractor:
    """
    Extracts and documents data sources, targets, and lineage from Informatica XML exports.
    Generates comprehensive documentation for migration to PySpark.
    """
    
    def __init__(self, spark: SparkSession, xml_path: str, output_path: str):
        """
        Initialize the metadata extractor.
        
        Args:
            spark: SparkSession instance
            xml_path: Path to Informatica XML export files
            output_path: Path for output documentation
        """
        self.spark = spark
        self.xml_path = xml_path
        self.output_path = output_path
        self.sources = []
        self.targets = []
        self.transformations = []
        self.connections = {}
        self.workflows = []
        self.lineage = []
        
    def extract_connections_from_xml(self, xml_file: str) -> Dict[str, Any]:
        """
        Extract source and target connection definitions from Informatica XML.
        
        Args:
            xml_file: Path to XML file
            
        Returns:
            Dictionary containing connection details
        """
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            connections = {}
            
            # Extract relational connections
            for conn in root.findall(".//CONNECTION[@TYPE='Relational']") or []:
                conn_name = conn.get('NAME', '')
                connections[conn_name] = {
                    'name': conn_name,
                    'type': 'Relational',
                    'db_type': conn.get('DATABASETYPE', ''),
                    'server': conn.get('SERVERNAME', ''),
                    'database': conn.get('DATABASENAME', ''),
                    'schema': conn.get('SCHEMANAME', ''),
                    'username': conn.get('USERNAME', ''),
                    'port': conn.get('PORT', ''),
                    'connection_string': conn.get('CONNECTIONSTRING', ''),
                    'extracted_date': datetime.now().isoformat()
                }
            
            # Extract file connections
            for conn in root.findall(".//CONNECTION[@TYPE='FlatFile']") or []:
                conn_name = conn.get('NAME', '')
                connections[conn_name] = {
                    'name': conn_name,
                    'type': 'FlatFile',
                    'file_path': conn.get('FILEPATH', ''),
                    'delimiter': conn.get('DELIMITER', ''),
                    'format': conn.get('FILEFORMAT', ''),
                    'extracted_date': datetime.now().isoformat()
                }
            
            # Extract FTP/SFTP connections
            for conn in root.findall(".//CONNECTION[@TYPE='FTP']") or []:
                conn_name = conn.get('NAME', '')
                connections[conn_name] = {
                    'name': conn_name,
                    'type': 'FTP',
                    'host': conn.get('HOST', ''),
                    'port': conn.get('PORT', ''),
                    'username': conn.get('USERNAME', ''),
                    'directory': conn.get('REMOTEDIR', ''),
                    'extracted_date': datetime.now().isoformat()
                }
            
            # Extract web service connections
            for conn in root.findall(".//CONNECTION[@TYPE='WebService']") or []:
                conn_name = conn.get('NAME', '')
                connections[conn_name] = {
                    'name': conn_name,
                    'type': 'WebService',
                    'url': conn.get('URL', ''),
                    'endpoint': conn.get('ENDPOINT', ''),
                    'auth_type': conn.get('AUTHTYPE', ''),
                    'extracted_date': datetime.now().isoformat()
                }
            
            return connections
            
        except Exception as e:
            print(f"Error parsing XML file {xml_file}: {str(e)}")
            return {}
    
    def extract_sources(self, xml_file: str) -> List[Dict[str, Any]]:
        """
        Extract source definitions including tables, files, and APIs.
        
        Args:
            xml_file: Path to XML file
            
        Returns:
            List of source definitions
        """
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            sources = []
            
            # Extract source definitions
            for source in root.findall(".//SOURCE"):
                source_name = source.get('NAME', '')
                source_type = source.get('SOURCETYPE', '')
                
                source_info = {
                    'name': source_name,
                    'type': source_type,
                    'database_type': source.get('DATABASETYPE', ''),
                    'owner': source.get('OWNERNAME', ''),
                    'table_name': source.get('TABLENAME', ''),
                    'connection': source.get('DBDNAME', ''),
                    'fields': [],
                    'estimated_rows': source.get('NUMROWS', 0),
                    'extracted_date': datetime.now().isoformat()
                }
                
                # Extract field definitions
                for field in source.findall(".//SOURCEFIELD"):
                    field_info = {
                        'name': field.get('NAME', ''),
                        'datatype': field.get('DATATYPE', ''),
                        'precision': field.get('PRECISION', ''),
                        'scale': field.get('SCALE', ''),
                        'nullable': field.get('NULLABLE', 'YES'),
                        'key_type': field.get('KEYTYPE', ''),
                        'business_name': field.get('BUSINESSNAME', '')
                    }
                    source_info['fields'].append(field_info)
                
                sources.append(source_info)
            
            # Extract file sources
            for file_src in root.findall(".//FILESOURCE"):
                file_info = {
                    'name': file_src.get('NAME', ''),
                    'type': 'FlatFile',
                    'file_path': file_src.get('FILEPATH', ''),
                    'delimiter': file_src.get('DELIMITER', ''),
                    'line_separator': file_src.get('LINESEPARATOR', ''),
                    'text_qualifier': file_src.get('TEXTQUALIFIER', ''),
                    'header_rows': file_src.get('HEADERROWS', 0),
                    'fields': [],
                    'extracted_date': datetime.now().isoformat()
                }
                
                for field in file_src.findall(".//FIELD"):
                    field_info = {
                        'name': field.get('NAME', ''),
                        'datatype': field.get('DATATYPE', ''),
                        'precision': field.get('PRECISION', ''),
                        'position': field.get('POSITION', ''),
                        'length': field.get('LENGTH', '')
                    }
                    file_info['fields'].append(field_info)
                
                sources.append(file_info)
            
            return sources
            
        except Exception as e:
            print(f"Error extracting sources from {xml_file}: {str(e)}")
            return []
    
    def extract_targets(self, xml_file: str) -> List[Dict[str, Any]]:
        """
        Extract target definitions with write patterns and properties.
        
        Args:
            xml_file: Path to XML file
            
        Returns:
            List of target definitions
        """
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            targets = []
            
            for target in root.findall(".//TARGET"):
                target_name = target.get('NAME', '')
                
                target_info = {
                    'name': target_name,
                    'type': target.get('TARGETTYPE', ''),
                    'database_type': target.get('DATABASETYPE', ''),
                    'owner': target.get('OWNERNAME', ''),
                    'table_name': target.get('TABLENAME', ''),
                    'connection': target.get('DBDNAME', ''),
                    'write_mode': target.get('LOADTYPE', 'NORMAL'),
                    'truncate_target': target.get('TRUNCATETARGET', 'NO'),
                    'update_else_insert': target.get('UPDATEELSEINSERT', 'NO'),
                    'fields': [],
                    'constraints': [],
                    'indexes': [],
                    'extracted_date': datetime.now().isoformat()
                }
                
                # Extract field definitions
                for field in target.findall(".//TARGETFIELD"):
                    field_info = {
                        'name': field.get('NAME', ''),
                        'datatype': field.get('DATATYPE', ''),
                        'precision': field.get('PRECISION', ''),
                        'scale': field.get('SCALE', ''),
                        'nullable': field.get('NULLABLE', 'YES'),
                        'key_type': field.get('KEYTYPE', ''),
                        'default_value': field.get('DEFAULTVALUE', '')
                    }
                    target_info['fields'].append(field_info)
                
                # Extract constraints
                for constraint in target.findall(".//CONSTRAINT"):
                    constraint_info = {
                        'name': constraint.get('NAME', ''),
                        'type': constraint.get('TYPE', ''),
                        'columns': constraint.get('COLUMNS', '').split(',')
                    }
                    target_info['constraints'].append(constraint_info)
                
                targets.append(target_info)
            
            return targets
            
        except Exception as e:
            print(f"Error extracting targets from {xml_file}: {str(e)}")
            return []
    
    def extract_workflows(self, xml_file: str) -> List[Dict[str, Any]]:
        """
        Extract workflow definitions including scheduling and dependencies.
        
        Args:
            xml_file: Path to XML file
            
        Returns:
            List of workflow definitions
        """
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            workflows = []
            
            for workflow in root.findall(".//WORKFLOW"):
                workflow_name = workflow.get('NAME', '')
                
                workflow_info = {
                    'name': workflow_name,
                    'version': workflow.get('VERSIONNUMBER', ''),
                    'description': workflow.get('DESCRIPTION', ''),
                    'is_valid': workflow.get('ISVALID', 'YES'),
                    'start_time': workflow.get('STARTTIME', ''),
                    'end_time': workflow.get('ENDTIME', ''),
                    'schedule_name': workflow.get('SCHEDULERNAME', ''),
                    'run_status': workflow.get('RUNSTATUS', ''),
                    'tasks': [],
                    'sessions': [],
                    'dependencies': [],
                    'extracted_date': datetime.now().isoformat()
                }
                
                # Extract tasks
                for task in workflow.findall(".//TASK"):
                    task_info = {
                        'name': task.get('NAME', ''),
                        'type': task.get('TYPE', ''),
                        'reusable': task.get('REUSABLE', 'NO'),
                        'enabled': task.get('ISENABLED', 'YES')
                    }
                    workflow_info['tasks'].append(task_info)
                
                # Extract sessions
                for session in workflow.findall(".//SESSION"):
                    session_info = {
                        'name': session.get('NAME', ''),
                        'mapping': session.get('MAPPINGNAME', ''),
                        'reusable': session.get('REUSABLE', 'NO'),
                        'sort_order': session.get('SORTORDER', ''),
                        'source_connection': session.get('SOURCECONNECTION', ''),
                        'target_connection': session.get('TARGETCONNECTION', '')
                    }
                    workflow_info['sessions'].append(session_info)
                
                # Extract task dependencies
                for link in workflow.findall(".//TASKLINK"):
                    dependency_info = {
                        'from_task': link.get('FROMTASK', ''),
                        'to_task': link.get('TOTASK', ''),
                        'condition': link.get('CONDITION', '')
                    }
                    workflow_info['dependencies'].append(dependency_info)
                
                workflows.append(workflow_info)
            
            return workflows
            
        except Exception as e:
            print(f"Error extracting workflows from {xml_file}: {str(e)}")
            return []
    
    def extract_transformations(self, xml_file: str) -> List[Dict[str, Any]]:
        """
        Extract transformation logic for lineage and documentation.
        
        Args:
            xml_file: Path to XML file
            
        Returns:
            List of transformation definitions
        """
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            transformations = []
            
            for mapping in root.findall(".//MAPPING"):
                mapping_name = mapping.get('NAME', '')
                
                for trans in mapping.findall(".//TRANSFORMATION"):
                    trans_info = {
                        'mapping_name': mapping_name,
                        'name': trans.get('NAME', ''),
                        'type': trans.get('TYPE', ''),
                        'description': trans.get('DESCRIPTION', ''),
                        'input_fields': [],
                        'output_fields': [],
                        'expressions': [],
                        'filter_condition': '',
                        'extracted_date': datetime.now().isoformat()
                    }
                    
                    # Extract transformation fields
                    for field in trans.findall(".//TRANSFORMFIELD"):
                        field_info = {
                            'name': field.get('NAME', ''),
                            'datatype': field.get('DATATYPE', ''),
                            'precision': field.get('PRECISION', ''),
                            'port_type': field.get('PORTTYPE', ''),
                            'expression': field.get('EXPRESSION', ''),
                            'default_value': field.get('DEFAULTVALUE', '')
                        }
                        
                        if field.get('PORTTYPE') in ['INPUT', 'INPUT/OUTPUT']:
                            trans_info['input_fields'].append(field_info)
                        if field.get('PORTTYPE') in ['OUTPUT', 'INPUT/OUTPUT', 'VARIABLE']:
                            trans_info['output_fields'].append(field_info)
                        
                        if field.get('EXPRESSION'):
                            trans_info['expressions'].append({
                                'field': field.get('NAME', ''),
                                'expression': field.get('EXPRESSION', '')
                            })
                    
                    # Extract filter conditions
                    filter_cond = trans.find(".//FILTERCONDITION")
                    if filter_cond is not None:
                        trans_info['filter_condition'] = filter_cond.text or ''
                    
                    transformations.append(trans_info)
            
            return transformations
            
        except Exception as e:
            print(f"Error extracting transformations from {xml_file}: {str(e)}")
            return []
    
    def build_lineage(self) -> List[Dict[str, Any]]:
        """
        Build source-to-target lineage from extracted metadata.
        
        Returns:
            List of lineage mappings
        """
        lineage = []
        
        for trans in self.transformations:
            for input_field in trans['input_fields']:
                for output_field in trans['output_fields']:
                    lineage_entry = {
                        'mapping_name': trans['mapping_name'],
                        'transformation': trans['name'],
                        'transformation_type': trans['type'],
                        'source_field': input_field['name'],
                        'target_field': output_field['name'],
                        'expression': next(
                            (exp['expression'] for exp in trans['expressions'] 
                             if exp['field'] == output_field['name']), 
                            None
                        ),
                        'datatype_conversion': f"{input_field.get('datatype', '')} -> {output_field.get('datatype', '')}",
                        'created_date': datetime.now().isoformat()
                    }
                    lineage.append(lineage_entry)
        
        return lineage
    
    def analyze_data_volumes(self, connection_details: Dict[str, Any]) -> pd.DataFrame:
        """
        Analyze data volumes from source systems.
        
        Args:
            connection_details: Dictionary with connection parameters
            
        Returns:
            DataFrame with volume statistics
        """
        volume_stats = []
        
        for source in self.sources:
            if source.get('type') == 'Relational' and source.get('table_name'):
                try:
                    # Build connection URL based on database type
                    db_type = source.get('database_type', '').upper()
                    
                    if db_type in ['ORACLE', 'POSTGRES', 'MYSQL', 'SQLSERVER']:
                        # Read table metadata
                        # Note: In production, use actual connection details
                        volume_stat = {
                            'source_name': source['name'],
                            'table_name': source['table_name'],
                            'estimated_rows': source.get('estimated_rows', 0),
                            'field_count': len(source.get('fields', [])),
                            'connection': source.get('connection', ''),
                            'database_type': db_type,
                            'analysis_date': datetime.now().isoformat()
                        }
                        volume_stats.append(volume_stat)
                        
                except Exception as e:
                    print(f"Error analyzing volume for {source['name']}: {str(e)}")
        
        return pd.DataFrame(volume_stats)
    
    def document_data_quality_rules(self, xml_file: str) -> List[Dict[str, Any]]:
        """
        Extract data quality rules and constraints.
        
        Args:
            xml_file: Path to XML file
            
        Returns:
            List of data quality rules
        """
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            dq_rules = []
            
            # Extract constraints from sources and targets
            for source in root.findall(".//SOURCE") + root.findall(".//TARGET"):
                obj_name = source.get('NAME', '')
                
                for field in source.findall(".//SOURCEFIELD") + source.findall(".//TARGETFIELD"):
                    field_name = field.get('NAME', '')
                    
                    # Nullable constraint
                    if field.get('NULLABLE') == 'NO':
                        dq_rules.append({
                            'object': obj_name,
                            'field': field_name,
                            'rule_type': 'NOT_NULL',
                            'description': f"{field_name} must not be null",
                            'severity': 'CRITICAL'
                        })
                    
                    # Key constraints
                    if field.get('KEYTYPE') == 'PRIMARY KEY':
                        dq_rules.append({
                            'object': obj_name,
                            'field': field_name,
                            'rule_type': 'PRIMARY_KEY',
                            'description': f"{field_name} is a primary key",
                            'severity': 'CRITICAL'
                        })
                    
                    # Datatype constraints
                    datatype = field.get('DATATYPE', '')
                    precision = field.get('PRECISION', '')
                    scale = field.get('SCALE', '')
                    
                    if datatype and precision:
                        dq_rules.append({
                            'object': obj_name,
                            'field': field_name,
                            'rule_type': 'DATATYPE_VALIDATION',
                            'description': f"{field_name} must be {datatype}({precision},{scale})",
                            'severity': 'HIGH'
                        })
            
            # Extract validation rules from transformations
            for trans in root.findall(".//TRANSFORMATION[@TYPE='Filter']"):
                trans_name = trans.get('NAME', '')
                filter_cond = trans.find(".//FILTERCONDITION")
                
                if filter_cond is not None and filter_cond.text:
                    dq_rules.append({
                        'object': trans_name,
                        'field': 'N/A',
                        'rule_type': 'FILTER_CONDITION',
                        'description': filter_cond.text,
                        'severity': 'MEDIUM'
                    })
            
            return dq_rules
            
        except Exception as e:
            print(f"Error extracting DQ rules from {xml_file}: {str(e)}")
            return []
    
    def generate_documentation(self):
        """
        Generate comprehensive documentation in multiple formats.
        """
        # Create output directory
        os.makedirs(self.output_path, exist_ok=True)
        
        # 1. Connection Documentation
        conn_df = self.spark.createDataFrame(
            pd.DataFrame([v for v in self.connections.values()])
        )
        conn_df.write.mode('overwrite').parquet(
            f"{self.output_path}/connections"
        )
        conn_df.toPandas().to_csv(
            f"{self.output_path}/connections.csv", 
            index=False
        )
        
        # 2. Source Documentation
        sources_df = self.spark.createDataFrame(
            pd.DataFrame(self.sources)
        )
        sources_df.write.mode('overwrite').parquet(
            f"{self.output_path}/sources"
        )
        sources_df.toPandas().to_csv(
            f"{self.output_path}/sources.csv", 
            index=False
        )
        
        # 3. Target Documentation
        targets_df = self.spark.createDataFrame(
            pd.DataFrame(self.targets)
        )
        targets_df.write.mode('overwrite').parquet(
            f"{self.output_path}/targets"
        )
        targets_df.toPandas().to_csv(
            f"{self.output_path}/targets.csv", 
            index=False
        )
        
        # 4. Workflow Documentation
        workflows_df = self.spark.createDataFrame(
            pd.DataFrame(self.workflows)
        )
        workflows_df.write.mode('overwrite').parquet(
            f"{self.output_path}/workflows"
        )
        workflows_df.toPandas().to_csv(
            f"{self.output_path}/workflows.csv", 
            index=False
        )
        
        # 5. Transformation Documentation
        trans_df = self.spark.createDataFrame(
            pd.DataFrame(self.transformations)
        )
        trans_df.write.mode('overwrite').parquet(
            f"{self.output_path}/transformations"
        )
        trans_df.toPandas().to_csv(
            f"{self.output_path}/transformations.csv", 
            index=False
        )
        
        # 6. Lineage Documentation
        lineage_df = self.spark.createDataFrame(
            pd.DataFrame(self.lineage)
        )
        lineage_df.write.mode('overwrite').parquet(
            f"{self.output_path}/lineage"
        )
        lineage_df.toPandas().to_csv(
            f"{self.output_path}/lineage.csv", 
            index=False
        )
        
        # 7. Generate summary report
        self._generate_summary_report()
        
        # 8. Generate YAML configuration for PySpark migration
        self._generate_migration_config()
        
        print(f"Documentation generated successfully at {self.output_path}")
    
    def _generate_summary_report(self):
        """Generate executive summary report."""
        summary = {
            'extraction_date': datetime.now().isoformat(),
            'statistics': {
                'total_connections': len(self.connections),
                'total_sources': len(self.sources),
                'total_targets': len(self.targets),
                'total_workflows': len(self.workflows),
                'total_transformations': len(self.transformations),
                'lineage_mappings': len(self.lineage)
            },
            'connection_breakdown': defaultdict(int),
            'source_breakdown': defaultdict(int),
            'target_breakdown': defaultdict(int),
            'transformation_breakdown': defaultdict(int)
        }
        
        # Count by type
        for conn in self.connections.values():
            summary['connection_breakdown'][conn['type']] += 1
        
        for source in self.sources:
            summary['source_breakdown'][source['type']] += 1
        
        for target in self.targets:
            summary['target_breakdown'][target['type']] += 1
        
        for trans in self.transformations:
            summary['transformation_breakdown'][trans['type']] += 1
        
        # Write summary
        with open(f"{self.output_path}/summary_report.json", 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        # Generate markdown report
        markdown_report = self._generate_markdown_report(summary)
        with open(f"{self.output_path}/MIGRATION_REPORT.md", 'w') as f:
            f.write(markdown_report)
    
    def _generate_markdown_report(self, summary: Dict) -> str:
        """Generate markdown format report."""
        report = f"""# Informatica to PySpark Migration Report

## Extraction Summary
- **Extraction Date**: {summary['extraction_date']}
- **Total Connections**: {summary['statistics']['total_connections']}
- **Total Sources**: {summary['statistics']['total_sources']}
- **Total Targets**: {summary['statistics']['total_targets']}
- **Total Workflows**: {summary['statistics']['total_workflows']}
- **Total Transformations**: {summary['statistics']['total_transformations']}
- **Lineage Mappings**: {summary['statistics']['lineage_mappings']}

## Connection Breakdown
"""
        for conn_type, count in summary['connection_breakdown'].items():
            report += f"- **{conn_type}**: {count}\n"
        
        report += "\n## Source Systems\n"
        for src_type, count in summary['source_breakdown'].items():
            report += f"- **{src_type}**: {count}\n"
        
        report += "\n## Target Systems\n"
        for tgt_type, count in summary['target_breakdown'].items():
            report += f"- **{tgt_type}**: {count}\n"
        
        report += "\n## Transformation Types\n"
        for trans_type, count in summary['transformation_breakdown'].items():
            report += f"- **{trans_type}**: {count}\n"
        
        report += """
## Next Steps
1. Review extracted metadata in the output directory
2. Validate connection details and credentials
3. Analyze lineage mappings for complex transformations
4. Review data quality rules for migration
5. Create PySpark job templates based on workflows
6. Plan data volume testing strategy
7. Design error handling and logging framework

## Output Files
- `connections.csv` - All connection definitions
- `sources.csv` - Source system inventory
- `targets.csv` - Target system inventory
- `workflows.csv` - Workflow definitions
- `transformations.csv` - Transformation logic
- `lineage.csv` - Source-to-target lineage
- `summary_report.json` - Detailed statistics
"""
        return report
    
    def _generate_migration_config(self):
        """Generate YAML configuration for PySpark migration."""
        config = {
            'migration': {
                'source_system': 'Informatica',
                'target_system': 'PySpark',
                'extraction_date': datetime.now().isoformat()
            },
            'connections': {},
            'workflows': []
        }
        
        # Add connections
        for conn_name, conn_details in self.connections.items():
            config['connections'][conn_name] = {
                'type': conn_details.get('type'),
                'properties': {k: v for k, v in conn_details.items() 
                             if k not in ['name', 'type', 'username', 'extracted_date']}
            }
        
        # Add workflows with sources and targets
        for workflow in self.workflows:
            wf_config = {
                'name': workflow['name'],
                'description': workflow.get('description', ''),
                'schedule': workflow.get('schedule_name', ''),
                'sessions': []
            }
            
            for session in workflow.get('sessions', []):
                session_config = {
                    'name': session['name'],
                    'mapping': session.get('mapping', ''),
                    'source_connection': session.get('source_connection', ''),
                    'target_connection': session.get('target_connection', '')
                }
                wf_config['sessions'].append(session_config)
            
            config['workflows'].append(wf_config)
        
        # Write configuration
        with open(f"{self.output_path}/migration_config.yaml", 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    
    def process_all_xml_files(self):
        """
        Process all XML files in the input directory.
        """
        xml_files = [f for f in os.listdir(self.xml_path) if f.endswith('.xml')]
        
        print(f"Found {len(xml_files)} XML files to process")
        
        for xml_file in xml_files:
            file_path = os.path.join(self.xml_path, xml_file)
            print(f"Processing {xml_file}...")
            
            # Extract connections
            connections = self.extract_connections_from_xml(file_path)
            self.connections.update(connections)
            
            # Extract sources
            sources = self.extract_sources(file_path)
            self.sources.extend(sources)
            
            # Extract targets
            targets = self.extract_targets(file_path)
            self.targets.extend(targets)
            
            # Extract workflows
            workflows = self.extract_workflows(file_path)
            self.workflows.extend(workflows)
            
            # Extract transformations
            transformations = self.extract_transformations(file_path)
            self.transformations.extend(transformations)
        
        # Build lineage after all extractions
        print("Building lineage mappings...")
        self.lineage = self.build_lineage()
        
        # Generate documentation
        print("Generating documentation...")
        self.generate_documentation()


def main():
    """
    Main execution function for metadata extraction and documentation.
    """
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("InformaticaMetadataExtractor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Configuration
    xml_path = "/path/to/informatica/xml/exports"
    output_path = "/path/to/output/documentation"
    
    try:
        # Initialize extractor
        extractor = InformaticaMetadataExtractor(
            spark=spark,
            xml_path=xml_path,
            output_path=output_path
        )
        
        # Process all XML files
        extractor.process_all_xml_files()
        
        print("\n" + "="*80)