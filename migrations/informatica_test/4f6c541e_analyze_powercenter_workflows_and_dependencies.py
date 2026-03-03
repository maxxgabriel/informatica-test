import xml.etree.ElementTree as ET
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode, array, struct, collect_list, when, coalesce
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, BooleanType
from datetime import datetime
import networkx as nx
import matplotlib.pyplot as plt
from typing import Dict, List, Tuple, Any
import re
from collections import defaultdict
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PowerCenterWorkflowAnalyzer:
    """
    Analyzes PowerCenter XML workflows to extract metadata, dependencies,
    and orchestration patterns for migration to PySpark.
    """
    
    def __init__(self, spark: SparkSession, xml_directory: str, output_directory: str):
        """
        Initialize the analyzer with Spark session and directories.
        
        Args:
            spark: Active SparkSession
            xml_directory: Directory containing PowerCenter XML files
            output_directory: Directory for output artifacts
        """
        self.spark = spark
        self.xml_directory = xml_directory
        self.output_directory = output_directory
        self.workflows = []
        self.sessions = []
        self.dependencies = []
        self.parameters = []
        self.connections = []
        self.error_handlers = []
        self.schedules = []
        
        # Create output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Initialized PowerCenterWorkflowAnalyzer with output directory: {output_directory}")
    
    def parse_xml_files(self) -> None:
        """
        Parse all XML files in the specified directory to extract workflow metadata.
        """
        logger.info(f"Starting to parse XML files from: {self.xml_directory}")
        xml_files = [f for f in os.listdir(self.xml_directory) if f.endswith('.xml')]
        
        for xml_file in xml_files:
            file_path = os.path.join(self.xml_directory, xml_file)
            try:
                logger.info(f"Parsing file: {xml_file}")
                tree = ET.parse(file_path)
                root = tree.getroot()
                
                # Extract workflows
                self._extract_workflows(root, xml_file)
                
                # Extract sessions
                self._extract_sessions(root, xml_file)
                
                # Extract dependencies
                self._extract_dependencies(root, xml_file)
                
                # Extract parameters and variables
                self._extract_parameters(root, xml_file)
                
                # Extract connection details
                self._extract_connections(root, xml_file)
                
                # Extract error handling
                self._extract_error_handlers(root, xml_file)
                
                # Extract scheduling information
                self._extract_schedules(root, xml_file)
                
                logger.info(f"Successfully parsed: {xml_file}")
            except Exception as e:
                logger.error(f"Error parsing {xml_file}: {str(e)}")
                continue
        
        logger.info(f"Completed parsing {len(xml_files)} XML files")
    
    def _extract_workflows(self, root: ET.Element, source_file: str) -> None:
        """
        Extract workflow definitions from XML.
        
        Args:
            root: XML root element
            source_file: Source XML filename
        """
        # Handle multiple XML schema variations
        workflow_tags = ['.//WORKFLOW', './/Workflow', './/workflow']
        
        for tag in workflow_tags:
            for workflow in root.findall(tag):
                workflow_data = {
                    'workflow_name': workflow.get('NAME', workflow.get('name', 'Unknown')),
                    'workflow_description': workflow.get('DESCRIPTION', workflow.get('description', '')),
                    'workflow_type': workflow.get('WORKFLOW_TYPE', workflow.get('type', 'Standard')),
                    'version': workflow.get('VERSION', workflow.get('version', '1')),
                    'is_valid': workflow.get('ISVALID', workflow.get('isValid', 'YES')),
                    'server_name': workflow.get('SERVER_NAME', workflow.get('serverName', '')),
                    'run_mode': workflow.get('RUN_MODE', workflow.get('runMode', 'Normal')),
                    'concurrent_execution': workflow.get('CONCURRENT_EXECUTION', workflow.get('concurrentExecution', 'NO')),
                    'source_file': source_file,
                    'extraction_timestamp': datetime.now().isoformat()
                }
                
                # Extract workflow variables
                workflow_variables = []
                for var_tag in ['.//WORKFLOWVARIABLE', './/WorkflowVariable', './/workflowVariable']:
                    for var in workflow.findall(var_tag):
                        workflow_variables.append({
                            'name': var.get('NAME', var.get('name', '')),
                            'datatype': var.get('DATATYPE', var.get('dataType', '')),
                            'default_value': var.get('DEFAULTVALUE', var.get('defaultValue', '')),
                            'is_user_defined': var.get('ISUSERDEFINED', var.get('isUserDefined', 'NO'))
                        })
                
                workflow_data['workflow_variables'] = workflow_variables
                
                # Extract tasks within workflow
                tasks = []
                for task_tag in ['.//TASK', './/Task', './/task']:
                    for task in workflow.findall(task_tag):
                        tasks.append({
                            'task_name': task.get('NAME', task.get('name', '')),
                            'task_type': task.get('TYPE', task.get('type', '')),
                            'reusable': task.get('REUSABLE', task.get('reusable', 'NO')),
                            'instance_name': task.get('INSTANCENAME', task.get('instanceName', ''))
                        })
                
                workflow_data['tasks'] = tasks
                self.workflows.append(workflow_data)
    
    def _extract_sessions(self, root: ET.Element, source_file: str) -> None:
        """
        Extract session configurations from XML.
        
        Args:
            root: XML root element
            source_file: Source XML filename
        """
        session_tags = ['.//SESSION', './/Session', './/session']
        
        for tag in session_tags:
            for session in root.findall(tag):
                session_data = {
                    'session_name': session.get('NAME', session.get('name', 'Unknown')),
                    'mapping_name': session.get('MAPPINGNAME', session.get('mappingName', '')),
                    'session_type': session.get('SESSIONTYPE', session.get('sessionType', 'NonReusable')),
                    'is_valid': session.get('ISVALID', session.get('isValid', 'YES')),
                    'sort_order': session.get('SORTORDER', session.get('sortOrder', '')),
                    'source_file': source_file,
                    'extraction_timestamp': datetime.now().isoformat()
                }
                
                # Extract session configuration attributes
                config_attrs = []
                for attr_tag in ['.//ATTRIBUTE', './/Attribute', './/attribute']:
                    for attr in session.findall(attr_tag):
                        config_attrs.append({
                            'name': attr.get('NAME', attr.get('name', '')),
                            'value': attr.get('VALUE', attr.get('value', ''))
                        })
                
                session_data['config_attributes'] = config_attrs
                
                # Extract session components (sources, targets, transformations)
                components = []
                for comp_tag in ['.//SESSIONCOMPONENT', './/SessionComponent']:
                    for comp in session.findall(comp_tag):
                        components.append({
                            'component_name': comp.get('NAME', comp.get('name', '')),
                            'component_type': comp.get('TYPE', comp.get('type', '')),
                            'instance_name': comp.get('INSTANCENAME', comp.get('instanceName', ''))
                        })
                
                session_data['components'] = components
                self.sessions.append(session_data)
    
    def _extract_dependencies(self, root: ET.Element, source_file: str) -> None:
        """
        Extract workflow dependencies and execution order.
        
        Args:
            root: XML root element
            source_file: Source XML filename
        """
        # Extract task links/connections
        link_tags = ['.//TASKLINK', './/TaskLink', './/WORKFLOWLINK', './/WorkflowLink']
        
        for tag in link_tags:
            for link in root.findall(tag):
                dependency_data = {
                    'from_task': link.get('FROMTASK', link.get('fromTask', '')),
                    'to_task': link.get('TOTASK', link.get('toTask', '')),
                    'from_instance': link.get('FROMINSTANCE', link.get('fromInstance', '')),
                    'to_instance': link.get('TOINSTANCE', link.get('toInstance', '')),
                    'expression': link.get('EXPRESSION', link.get('expression', '')),
                    'link_type': link.get('TYPE', link.get('type', 'Sequential')),
                    'condition': link.get('CONDITION', link.get('condition', '')),
                    'source_file': source_file,
                    'extraction_timestamp': datetime.now().isoformat()
                }
                self.dependencies.append(dependency_data)
        
        # Extract workflow dependencies
        for workflow in root.findall('.//WORKFLOW'):
            workflow_name = workflow.get('NAME', '')
            
            # Extract parent workflow dependencies
            for parent_dep in workflow.findall('.//PARENTWORKFLOW'):
                dependency_data = {
                    'from_task': parent_dep.get('NAME', ''),
                    'to_task': workflow_name,
                    'from_instance': '',
                    'to_instance': '',
                    'expression': '',
                    'link_type': 'ParentChild',
                    'condition': '',
                    'source_file': source_file,
                    'extraction_timestamp': datetime.now().isoformat()
                }
                self.dependencies.append(dependency_data)
    
    def _extract_parameters(self, root: ET.Element, source_file: str) -> None:
        """
        Extract workflow parameters and variables.
        
        Args:
            root: XML root element
            source_file: Source XML filename
        """
        # Extract workflow parameters
        param_tags = ['.//WORKFLOWVARIABLE', './/SESSIONVARIABLE', './/MAPPINGVARIABLE']
        
        for tag in param_tags:
            for param in root.findall(tag):
                param_data = {
                    'parameter_name': param.get('NAME', param.get('name', '')),
                    'parameter_type': tag.split('}')[-1].replace('//', '').replace('.', ''),
                    'datatype': param.get('DATATYPE', param.get('dataType', '')),
                    'default_value': param.get('DEFAULTVALUE', param.get('defaultValue', '')),
                    'is_user_defined': param.get('ISUSERDEFINED', param.get('isUserDefined', 'NO')),
                    'is_nullable': param.get('ISNULLABLE', param.get('isNullable', 'YES')),
                    'precision': param.get('PRECISION', param.get('precision', '')),
                    'scale': param.get('SCALE', param.get('scale', '')),
                    'usage_type': param.get('USAGETYPE', param.get('usageType', '')),
                    'source_file': source_file,
                    'extraction_timestamp': datetime.now().isoformat()
                }
                
                # Find parent workflow/session
                parent = self._find_parent_element(root, param)
                if parent is not None:
                    param_data['parent_name'] = parent.get('NAME', parent.get('name', ''))
                    param_data['parent_type'] = parent.tag.split('}')[-1]
                
                self.parameters.append(param_data)
        
        # Extract parameter files
        for param_file in root.findall('.//PARAMETERFILE'):
            param_data = {
                'parameter_name': 'PARAMETER_FILE',
                'parameter_type': 'ParameterFile',
                'datatype': 'String',
                'default_value': param_file.get('NAME', ''),
                'is_user_defined': 'YES',
                'is_nullable': 'NO',
                'precision': '',
                'scale': '',
                'usage_type': 'Input',
                'source_file': source_file,
                'extraction_timestamp': datetime.now().isoformat()
            }
            self.parameters.append(param_data)
    
    def _extract_connections(self, root: ET.Element, source_file: str) -> None:
        """
        Extract connection and credential information.
        
        Args:
            root: XML root element
            source_file: Source XML filename
        """
        # Extract relational connections
        for conn in root.findall('.//CONNECTION'):
            connection_data = {
                'connection_name': conn.get('NAME', conn.get('name', '')),
                'connection_type': conn.get('TYPE', conn.get('type', '')),
                'connection_subtype': conn.get('SUBTYPE', conn.get('subType', '')),
                'username': conn.get('USERNAME', conn.get('userName', '')),
                'database_type': conn.get('DATABASETYPE', conn.get('databaseType', '')),
                'server_name': conn.get('SERVERNAME', conn.get('serverName', '')),
                'database_name': conn.get('DATABASENAME', conn.get('databaseName', '')),
                'schema_name': conn.get('SCHEMANAME', conn.get('schemaName', '')),
                'connection_string': conn.get('CONNECTIONSTRING', conn.get('connectionString', '')),
                'code_page': conn.get('CODEPAGE', conn.get('codePage', '')),
                'source_file': source_file,
                'extraction_timestamp': datetime.now().isoformat()
            }
            
            # Extract connection attributes
            attributes = []
            for attr in conn.findall('.//ATTRIBUTE'):
                attributes.append({
                    'name': attr.get('NAME', ''),
                    'value': attr.get('VALUE', '')
                })
            
            connection_data['attributes'] = attributes
            self.connections.append(connection_data)
        
        # Extract FTP connections
        for ftp_conn in root.findall('.//FTPCONNECTION'):
            connection_data = {
                'connection_name': ftp_conn.get('NAME', ''),
                'connection_type': 'FTP',
                'connection_subtype': ftp_conn.get('FTPTYPE', ''),
                'username': ftp_conn.get('USERNAME', ''),
                'server_name': ftp_conn.get('HOSTNAME', ''),
                'port': ftp_conn.get('PORT', ''),
                'source_file': source_file,
                'extraction_timestamp': datetime.now().isoformat()
            }
            self.connections.append(connection_data)
    
    def _extract_error_handlers(self, root: ET.Element, source_file: str) -> None:
        """
        Extract error handling and recovery mechanisms.
        
        Args:
            root: XML root element
            source_file: Source XML filename
        """
        # Extract session-level error handling
        for session in root.findall('.//SESSION'):
            session_name = session.get('NAME', '')
            
            # Extract error handling configuration
            error_config = {
                'object_name': session_name,
                'object_type': 'SESSION',
                'source_file': source_file,
                'extraction_timestamp': datetime.now().isoformat()
            }
            
            # Extract configuration attributes related to error handling
            for attr in session.findall('.//ATTRIBUTE'):
                attr_name = attr.get('NAME', '')
                attr_value = attr.get('VALUE', '')
                
                if any(keyword in attr_name.upper() for keyword in 
                       ['ERROR', 'FAIL', 'RECOVER', 'ROLLBACK', 'COMMIT', 'TRAP']):
                    error_config[attr_name] = attr_value
            
            if len(error_config) > 4:  # More than just base fields
                self.error_handlers.append(error_config)
        
        # Extract workflow-level error handling
        for workflow in root.findall('.//WORKFLOW'):
            workflow_name = workflow.get('NAME', '')
            
            error_config = {
                'object_name': workflow_name,
                'object_type': 'WORKFLOW',
                'source_file': source_file,
                'extraction_timestamp': datetime.now().isoformat()
            }
            
            # Extract workflow attributes related to error handling
            for attr in workflow.findall('.//ATTRIBUTE'):
                attr_name = attr.get('NAME', '')
                attr_value = attr.get('VALUE', '')
                
                if any(keyword in attr_name.upper() for keyword in 
                       ['ERROR', 'FAIL', 'RECOVER', 'SUSPEND', 'ABORT', 'STOP']):
                    error_config[attr_name] = attr_value
            
            # Extract task recovery settings
            for task in workflow.findall('.//TASK'):
                task_name = task.get('NAME', '')
                for attr in task.findall('.//ATTRIBUTE'):
                    attr_name = attr.get('NAME', '')
                    if 'RECOVERY' in attr_name.upper() or 'FAIL' in attr_name.upper():
                        error_config[f"{task_name}_{attr_name}"] = attr.get('VALUE', '')
            
            if len(error_config) > 4:
                self.error_handlers.append(error_config)
    
    def _extract_schedules(self, root: ET.Element, source_file: str) -> None:
        """
        Extract scheduling patterns and triggers.
        
        Args:
            root: XML root element
            source_file: Source XML filename
        """
        # Extract scheduler definitions
        for scheduler in root.findall('.//SCHEDULER'):
            schedule_data = {
                'scheduler_name': scheduler.get('NAME', ''),
                'schedule_type': scheduler.get('SCHEDULETYPE', scheduler.get('scheduleType', '')),
                'start_time': scheduler.get('STARTTIME', scheduler.get('startTime', '')),
                'end_time': scheduler.get('ENDTIME', scheduler.get('endTime', '')),
                'repeat_interval': scheduler.get('REPEATINTERVAL', scheduler.get('repeatInterval', '')),
                'schedule_status': scheduler.get('STATUS', scheduler.get('status', '')),
                'source_file': source_file,
                'extraction_timestamp': datetime.now().isoformat()
            }
            
            # Extract schedule attributes
            attributes = []
            for attr in scheduler.findall('.//ATTRIBUTE'):
                attributes.append({
                    'name': attr.get('NAME', ''),
                    'value': attr.get('VALUE', '')
                })
            
            schedule_data['attributes'] = attributes
            self.schedules.append(schedule_data)
        
        # Extract workflow scheduling information
        for workflow in root.findall('.//WORKFLOW'):
            workflow_name = workflow.get('NAME', '')
            
            # Check for scheduling attributes
            schedule_attrs = {}
            for attr in workflow.findall('.//ATTRIBUTE'):
                attr_name = attr.get('NAME', '')
                if any(keyword in attr_name.upper() for keyword in 
                       ['SCHEDULE', 'TIMER', 'TRIGGER', 'INTERVAL', 'FREQUENCY']):
                    schedule_attrs[attr_name] = attr.get('VALUE', '')
            
            if schedule_attrs:
                schedule_data = {
                    'scheduler_name': workflow_name,
                    'schedule_type': 'Workflow',
                    'source_file': source_file,
                    'extraction_timestamp': datetime.now().isoformat()
                }
                schedule_data.update(schedule_attrs)
                self.schedules.append(schedule_data)
    
    def _find_parent_element(self, root: ET.Element, child: ET.Element) -> ET.Element:
        """
        Find the parent element of a given child element.
        
        Args:
            root: XML root element
            child: Child element to find parent for
            
        Returns:
            Parent element or None
        """
        parent_map = {c: p for p in root.iter() for c in p}
        return parent_map.get(child)
    
    def create_dataframes(self) -> Dict[str, Any]:
        """
        Create Spark DataFrames from extracted metadata.
        
        Returns:
            Dictionary of DataFrames
        """
        logger.info("Creating Spark DataFrames from extracted metadata")
        
        dataframes = {}
        
        # Create workflows DataFrame
        if self.workflows:
            workflows_df = self.spark.createDataFrame(
                [{k: v if not isinstance(v, list) else json.dumps(v) 
                  for k, v in wf.items()} for wf in self.workflows]
            )
            dataframes['workflows'] = workflows_df
            logger.info(f"Created workflows DataFrame with {workflows_df.count()} records")
        
        # Create sessions DataFrame
        if self.sessions:
            sessions_df = self.spark.createDataFrame(
                [{k: v if not isinstance(v, list) else json.dumps(v) 
                  for k, v in sess.items()} for sess in self.sessions]
            )
            dataframes['sessions'] = sessions_df
            logger.info(f"Created sessions DataFrame with {sessions_df.count()} records")
        
        # Create dependencies DataFrame
        if self.dependencies:
            dependencies_df = self.spark.createDataFrame(self.dependencies)
            dataframes['dependencies'] = dependencies_df
            logger.info(f"Created dependencies DataFrame with {dependencies_df.count()} records")
        
        # Create parameters DataFrame
        if self.parameters:
            parameters_df = self.spark.createDataFrame(self.parameters)
            dataframes['parameters'] = parameters_df
            logger.info(f"Created parameters DataFrame with {parameters_df.count()} records")
        
        # Create connections DataFrame
        if self.connections:
            connections_df = self.spark.createDataFrame(
                [{k: v if not isinstance(v, list) else json.dumps(v) 
                  for k, v in conn.items()} for conn in self.connections]
            )
            dataframes['connections'] = connections_df
            logger.info(f"Created connections DataFrame with {connections_df.count()} records")
        
        # Create error handlers DataFrame
        if self.error_handlers:
            error_handlers_df = self.spark.createDataFrame(self.error_handlers)
            dataframes['error_handlers'] = error_handlers_df
            logger.info(f"Created error_handlers DataFrame with {error_handlers_df.count()} records")
        
        # Create schedules DataFrame
        if self.schedules:
            schedules_df = self.spark.createDataFrame(
                [{k: v if not isinstance(v, list) else json.dumps(v) 
                  for k, v in sched.items()} for sched in self.schedules]
            )
            dataframes['schedules'] = schedules_df
            logger.info(f"Created schedules DataFrame with {schedules_df.count()} records")
        
        return dataframes
    
    def generate_dependency_tree(self) -> nx.DiGraph:
        """
        Generate directed graph of workflow dependencies.
        
        Returns:
            NetworkX directed graph
        """
        logger.info("Generating workflow dependency tree")
        
        G = nx.DiGraph()
        
        # Add nodes for all workflows
        for workflow in self.workflows:
            workflow_name = workflow['workflow_name']
            G.add_node(workflow_name, 
                      type='workflow',
                      description=workflow.get('workflow_description', ''),
                      run_mode=workflow.get('run_mode', ''))
        
        # Add nodes for all sessions
        for session in self.sessions:
            session_name = session['session_name']
            G.add_node(session_name,
                      type='session',
                      mapping=session.get('mapping_name', ''))
        
        # Add edges based on dependencies
        for dep in self.dependencies:
            from_task = dep.get('from_task', '')
            to_task = dep.get('to_task', '')
            
            if from_task and to_task:
                G.add_edge(from_task, to_task,
                          link_type=dep.get('link_type', ''),
                          condition=dep.get('condition', ''),
                          expression=dep.get('expression', ''))
        
        logger.info(f"Created dependency graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
        
        return G
    
    def visualize_dependency_tree(self, graph: nx.DiGraph, output_filename: str = 'workflow_dependencies.png') -> None:
        """
        Visualize and save workflow dependency graph.
        
        Args:
            graph: NetworkX directed graph
            output_filename: Output filename for the visualization
        """
        logger.info(f"Visualizing dependency tree to {output_filename}")
        
        plt.figure(figsize=(20, 12))
        
        # Use hierarchical layout for better visualization
        try:
            pos = nx.spring_layout(graph, k=2, iterations=50)
        except:
            pos = nx.shell_layout(graph)
        
        # Color nodes by type
        node_colors = []
        for node in graph.nodes():
            node_type = graph.nodes[node].get('type', 'unknown')
            if node_type == 'workflow':
                node_colors.append('lightblue')
            elif node_type == 'session':
                node_colors.append('lightgreen')
            else:
                node_colors.append('lightgray')
        
        # Draw the graph
        nx.draw(graph, pos, 
               node_color=node_colors,
               node_size=3000,
               with_labels=True,
               font_size=8,
               font_weight='bold',
               arrows=True,
               arrowsize=20,
               edge_color='gray',
               alpha=0.7)
        
        plt.title("Workflow Dependency Tree", fontsize=16, fontweight='bold')
        
        # Add legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='lightblue', label='Workflow'),
            Patch(facecolor='lightgreen', label='Session')
        ]
        plt.legend(handles=legend_elements, loc='upper right')
        
        # Save the figure
        output_path = os.path.join(self.output_directory, output_filename)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Dependency tree visualization saved to {output_path}")
    
    def analyze_execution_order(self, graph: nx.DiGraph) -> List[List[str]]:
        """
        Determine execution order using topological sort.
        
        Args:
            graph: NetworkX directed graph
            
        Returns:
            List of execution levels (tasks that can run in parallel)
        """
        logger.info("Analyzing workflow execution order")
        
        execution_order = []
        
        try:
            # Find nodes with no dependencies (starting points)
            start_nodes = [node for node in graph.nodes() if graph.in_degree(node) == 0]
            
            # Perform topological sort with levels
            levels = []
            remaining_graph = graph.copy()
            
            while remaining_graph.number_of_nodes() > 0:
                # Find nodes with no incoming edges
                current_level = [node for node in remaining_graph.nodes() 
                               if remaining_graph.in_degree(node) == 0]
                
                if not current_level:
                    # Graph has cycles, break
                    logger.warning("Detected cycles in workflow dependencies")
                    break
                
                levels.append(current_level)
                remaining_graph.remove_nodes_from(current_level)
            
            execution_order = levels
            logger.info(f"Identified {len(execution_order)} execution levels")
            
        except nx.NetworkXError as e:
            logger.error(f"Error analyzing execution order: {str(e)}")
        
        return execution_order
    
    def generate_catalog_report(self, dataframes: Dict[str, Any]) -> None:
        """
        Generate comprehensive catalog report with all workflows and metadata.
        
        Args:
            dataframes: Dictionary of DataFrames
        """
        logger.info("Generating catalog report")
        
        report_path = os.path.join(self.output_directory, 'workflow_catalog.txt')
        
        with open(report_path, 'w') as f:
            f.write("=" * 100 + "\n")
            f.write("POWERCENTRE WORKFLOW CATALOG REPORT\n")
            f.write("=" * 100 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Source Directory: {self.xml_directory}\n\n")
            
            # Workflow Summary
            f.write("\n" + "=" * 100 + "\n")
            f.write("WORKFLOW SUMMARY\n")
            f.write("=" * 100 + "\n")
            if 'workflows' in dataframes:
                wf_df = dataframes['workflows']
                f.write(f"Total Workflows: {wf_df.count()}\n\n")
                
                wf_df.select('workflow_name', 'workflow_type', 'run_mode', 'concurrent_execution').show(
                    truncate=False
                )
            
            # Session Summary
            f.write("\n" + "=" * 100 + "\n")
            f.write("SESSION SUMMARY\n")
            f.write("=" * 100 + "\n")
            if 'sessions' in dataframes:
                sess_df = dataframes['sessions']
                f.write(f"Total Sessions: {sess_df.count()}\n\n")
            
            # Dependency Summary
            f.write("\n" + "=" * 100 + "\n")
            f.write("DEPENDENCY SUMMARY\n")
            f.write("=" * 100 + "\n")
            if 'dependencies' in dataframes:
                dep_df = dataframes['dependencies']
                f.write(f"Total Dependencies: {dep_df.count()}\n\n")
                
                # Group by link type
                dep_df.groupBy('link_type').count().orderBy('count', ascending=False).show()
            
            # Parameter Summary
            f.write("\n" + "=" * 100 + "\n")
            f.write("PARAMETER SUMMARY\n")
            f.write("=" * 100 + "\n")
            if 'parameters' in dataframes:
                param_df = dataframes['parameters']
                f.write(f"Total Parameters: {param_df.count()}\n\n")
                
                # Group by parameter type
                param_df.groupBy('parameter_type').count().orderBy('count', ascending=False).show()
            
            # Connection Summary
            f.write("\n" + "=" * 100 + "\n")
            f.write("CONNECTION SUMMARY\n")
            f.write("=" * 100 + "\n")