import xml.etree.ElementTree as ET
import json
import os
from typing import Dict, List, Set, Tuple, Any
from dataclasses import dataclass, asdict
from collections import defaultdict
import graphviz
from datetime import datetime
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, IntegerType
from pyspark.sql.functions import col, explode, collect_list, struct, to_json


@dataclass
class WorkflowConnection:
    connection_name: str
    connection_type: str
    username: str
    database: str
    server: str
    codepage: str
    connection_object: str


@dataclass
class WorkflowParameter:
    parameter_name: str
    parameter_type: str
    default_value: str
    is_input: bool
    is_output: bool


@dataclass
class WorkflowVariable:
    variable_name: str
    variable_type: str
    default_value: str
    is_user_defined: bool
    persistence: str


@dataclass
class SessionConfig:
    session_name: str
    mapping_name: str
    source_connections: List[str]
    target_connections: List[str]
    session_log_file: str
    error_log_type: str
    commit_interval: int
    partition_type: str
    session_parameters: List[WorkflowParameter]


@dataclass
class ErrorHandling:
    error_type: str
    on_error_action: str
    recovery_strategy: str
    email_notification: bool
    notification_recipients: List[str]
    suspend_on_error: bool
    task_name: str


@dataclass
class ScheduleInfo:
    schedule_name: str
    schedule_type: str
    start_time: str
    end_time: str
    recurrence_pattern: str
    days_of_week: List[str]
    is_enabled: bool


@dataclass
class WorkflowTask:
    task_name: str
    task_type: str
    task_instance_name: str
    reusable: bool
    description: str
    fail_parent_if_fails: bool
    success_email_recipients: List[str]
    failure_email_recipients: List[str]
    task_dependencies: List[str]


@dataclass
class WorkflowDefinition:
    workflow_name: str
    workflow_version: str
    folder_name: str
    description: str
    is_valid: bool
    is_enabled: bool
    start_task: str
    tasks: List[WorkflowTask]
    parameters: List[WorkflowParameter]
    variables: List[WorkflowVariable]
    connections: List[WorkflowConnection]
    sessions: List[SessionConfig]
    schedule: ScheduleInfo
    error_handling: List[ErrorHandling]
    dependencies: List[str]
    orchestration_pattern: str


@dataclass
class DependencyRelationship:
    source_workflow: str
    target_workflow: str
    dependency_type: str
    condition: str
    execution_order: int


class PowerCenterWorkflowAnalyzer:
    """
    Comprehensive analyzer for PowerCenter workflows to extract definitions,
    dependencies, scheduling, parameters, and orchestration patterns
    """
    
    def __init__(self, xml_directory: str, output_directory: str):
        self.xml_directory = xml_directory
        self.output_directory = output_directory
        self.workflows: Dict[str, WorkflowDefinition] = {}
        self.dependencies: List[DependencyRelationship] = []
        self.spark = SparkSession.builder \
            .appName("PowerCenterWorkflowAnalyzer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        os.makedirs(output_directory, exist_ok=True)
        
        self.namespace = {
            'pmwf': 'http://www.informatica.com/powercenter/workflow',
            'pmrep': 'http://www.informatica.com/powercenter/repository'
        }
    
    def parse_all_workflows(self) -> Dict[str, WorkflowDefinition]:
        """Parse all PowerCenter XML files and extract workflow definitions"""
        
        print(f"Scanning directory: {self.xml_directory}")
        xml_files = self._discover_xml_files()
        
        print(f"Found {len(xml_files)} XML files to process")
        
        for xml_file in xml_files:
            try:
                print(f"Processing: {xml_file}")
                self._parse_workflow_xml(xml_file)
            except Exception as e:
                print(f"Error processing {xml_file}: {str(e)}")
                continue
        
        print(f"Successfully parsed {len(self.workflows)} workflows")
        return self.workflows
    
    def _discover_xml_files(self) -> List[str]:
        """Discover all XML files in the directory recursively"""
        xml_files = []
        
        for root, dirs, files in os.walk(self.xml_directory):
            for file in files:
                if file.endswith('.xml') or file.endswith('.XML'):
                    xml_files.append(os.path.join(root, file))
        
        return xml_files
    
    def _parse_workflow_xml(self, xml_file: str):
        """Parse individual workflow XML file"""
        
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
        except ET.ParseError as e:
            print(f"XML Parse Error in {xml_file}: {str(e)}")
            return
        
        workflows = root.findall('.//WORKFLOW')
        
        for workflow_elem in workflows:
            workflow_def = self._extract_workflow_definition(workflow_elem, xml_file)
            if workflow_def:
                self.workflows[workflow_def.workflow_name] = workflow_def
    
    def _extract_workflow_definition(self, workflow_elem: ET.Element, source_file: str) -> WorkflowDefinition:
        """Extract complete workflow definition from XML element"""
        
        workflow_name = workflow_elem.get('NAME', 'UNKNOWN')
        workflow_version = workflow_elem.get('VERSION', '1')
        folder_name = self._extract_folder_name(workflow_elem)
        description = workflow_elem.get('DESCRIPTION', '')
        is_valid = workflow_elem.get('ISVALID', '1') == '1'
        is_enabled = workflow_elem.get('ISENABLED', '1') == '1'
        
        tasks = self._extract_tasks(workflow_elem)
        start_task = self._identify_start_task(workflow_elem)
        parameters = self._extract_parameters(workflow_elem)
        variables = self._extract_variables(workflow_elem)
        connections = self._extract_connections(workflow_elem)
        sessions = self._extract_sessions(workflow_elem)
        schedule = self._extract_schedule(workflow_elem)
        error_handling = self._extract_error_handling(workflow_elem)
        dependencies = self._extract_workflow_dependencies(workflow_elem)
        orchestration_pattern = self._identify_orchestration_pattern(tasks)
        
        return WorkflowDefinition(
            workflow_name=workflow_name,
            workflow_version=workflow_version,
            folder_name=folder_name,
            description=description,
            is_valid=is_valid,
            is_enabled=is_enabled,
            start_task=start_task,
            tasks=tasks,
            parameters=parameters,
            variables=variables,
            connections=connections,
            sessions=sessions,
            schedule=schedule,
            error_handling=error_handling,
            dependencies=dependencies,
            orchestration_pattern=orchestration_pattern
        )
    
    def _extract_folder_name(self, workflow_elem: ET.Element) -> str:
        """Extract folder name from workflow"""
        
        parent = workflow_elem.find('..')
        if parent is not None and parent.tag == 'FOLDER':
            return parent.get('NAME', 'DEFAULT')
        
        return workflow_elem.get('FOLDERNAME', 'DEFAULT')
    
    def _extract_tasks(self, workflow_elem: ET.Element) -> List[WorkflowTask]:
        """Extract all tasks from workflow"""
        
        tasks = []
        task_elements = workflow_elem.findall('.//TASK')
        
        for task_elem in task_elements:
            task = self._parse_task(task_elem)
            tasks.append(task)
        
        return tasks
    
    def _parse_task(self, task_elem: ET.Element) -> WorkflowTask:
        """Parse individual task element"""
        
        task_name = task_elem.get('NAME', '')
        task_type = task_elem.get('TYPE', 'SESSION')
        task_instance_name = task_elem.get('INSTANCENAME', task_name)
        reusable = task_elem.get('REUSABLE', 'NO') == 'YES'
        description = task_elem.get('DESCRIPTION', '')
        fail_parent = task_elem.get('FAILPARENTIFFAILS', 'YES') == 'YES'
        
        success_email = self._extract_email_recipients(task_elem, 'SUCCESS')
        failure_email = self._extract_email_recipients(task_elem, 'FAILURE')
        
        dependencies = self._extract_task_dependencies(task_elem)
        
        return WorkflowTask(
            task_name=task_name,
            task_type=task_type,
            task_instance_name=task_instance_name,
            reusable=reusable,
            description=description,
            fail_parent_if_fails=fail_parent,
            success_email_recipients=success_email,
            failure_email_recipients=failure_email,
            task_dependencies=dependencies
        )
    
    def _extract_email_recipients(self, task_elem: ET.Element, email_type: str) -> List[str]:
        """Extract email recipients for task notifications"""
        
        recipients = []
        email_elem = task_elem.find(f'.//EMAILOPTION[@TYPE="{email_type}"]')
        
        if email_elem is not None:
            email_to = email_elem.get('EMAILTO', '')
            if email_to:
                recipients = [email.strip() for email in email_to.split(',')]
        
        return recipients
    
    def _extract_task_dependencies(self, task_elem: ET.Element) -> List[str]:
        """Extract task dependencies from TASKINSTANCE links"""
        
        dependencies = []
        parent = task_elem.find('..')
        
        if parent is not None:
            links = parent.findall('.//TASKINSTANCE')
            task_name = task_elem.get('NAME')
            
            for link in links:
                to_task = link.get('TOTASK', '')
                from_task = link.get('FROMTASK', '')
                
                if to_task == task_name and from_task:
                    dependencies.append(from_task)
        
        return dependencies
    
    def _identify_start_task(self, workflow_elem: ET.Element) -> str:
        """Identify the start task of the workflow"""
        
        start_task_elem = workflow_elem.find('.//TASK[@TYPE="Start"]')
        if start_task_elem is not None:
            return start_task_elem.get('NAME', '')
        
        task_instance = workflow_elem.find('.//TASKINSTANCE[@FROMTASK="Start"]')
        if task_instance is not None:
            return task_instance.get('TOTASK', '')
        
        tasks = workflow_elem.findall('.//TASK')
        if tasks:
            return tasks[0].get('NAME', '')
        
        return ''
    
    def _extract_parameters(self, workflow_elem: ET.Element) -> List[WorkflowParameter]:
        """Extract workflow parameters"""
        
        parameters = []
        param_elements = workflow_elem.findall('.//WORKFLOWVARIABLE[@ISPERSISTENT="NO"]')
        
        for param_elem in param_elements:
            param = WorkflowParameter(
                parameter_name=param_elem.get('NAME', ''),
                parameter_type=param_elem.get('DATATYPE', 'STRING'),
                default_value=param_elem.get('DEFAULTVALUE', ''),
                is_input=param_elem.get('ISINPUT', 'NO') == 'YES',
                is_output=param_elem.get('ISOUTPUT', 'NO') == 'YES'
            )
            parameters.append(param)
        
        return parameters
    
    def _extract_variables(self, workflow_elem: ET.Element) -> List[WorkflowVariable]:
        """Extract workflow variables"""
        
        variables = []
        var_elements = workflow_elem.findall('.//WORKFLOWVARIABLE')
        
        for var_elem in var_elements:
            persistence = var_elem.get('ISPERSISTENT', 'NO')
            
            variable = WorkflowVariable(
                variable_name=var_elem.get('NAME', ''),
                variable_type=var_elem.get('DATATYPE', 'STRING'),
                default_value=var_elem.get('DEFAULTVALUE', ''),
                is_user_defined=var_elem.get('ISUSERDEFINED', 'YES') == 'YES',
                persistence=persistence
            )
            variables.append(variable)
        
        return variables
    
    def _extract_connections(self, workflow_elem: ET.Element) -> List[WorkflowConnection]:
        """Extract connection objects used in workflow"""
        
        connections = []
        connection_elements = workflow_elem.findall('.//CONNECTION')
        
        for conn_elem in connection_elements:
            connection = WorkflowConnection(
                connection_name=conn_elem.get('NAME', ''),
                connection_type=conn_elem.get('TYPE', ''),
                username=conn_elem.get('USERNAME', ''),
                database=conn_elem.get('DATABASENAME', ''),
                server=conn_elem.get('SERVERNAME', ''),
                codepage=conn_elem.get('CODEPAGE', 'UTF-8'),
                connection_object=conn_elem.get('CONNECTIONOBJECT', '')
            )
            connections.append(connection)
        
        return connections
    
    def _extract_sessions(self, workflow_elem: ET.Element) -> List[SessionConfig]:
        """Extract session configurations"""
        
        sessions = []
        session_elements = workflow_elem.findall('.//SESSION')
        
        for session_elem in session_elements:
            session = self._parse_session_config(session_elem)
            sessions.append(session)
        
        return sessions
    
    def _parse_session_config(self, session_elem: ET.Element) -> SessionConfig:
        """Parse individual session configuration"""
        
        session_name = session_elem.get('NAME', '')
        mapping_name = session_elem.get('MAPPINGNAME', '')
        
        source_connections = self._extract_session_connections(session_elem, 'SOURCE')
        target_connections = self._extract_session_connections(session_elem, 'TARGET')
        
        session_log_file = session_elem.get('SESSIONLOGFILE', '')
        error_log_type = session_elem.get('ERRORLOGTYPE', 'RELATIONAL')
        
        commit_interval = int(session_elem.get('COMMITINTERVAL', '10000'))
        partition_type = session_elem.get('PARTITIONTYPE', 'NONE')
        
        session_params = self._extract_session_parameters(session_elem)
        
        return SessionConfig(
            session_name=session_name,
            mapping_name=mapping_name,
            source_connections=source_connections,
            target_connections=target_connections,
            session_log_file=session_log_file,
            error_log_type=error_log_type,
            commit_interval=commit_interval,
            partition_type=partition_type,
            session_parameters=session_params
        )
    
    def _extract_session_connections(self, session_elem: ET.Element, conn_type: str) -> List[str]:
        """Extract source or target connections from session"""
        
        connections = []
        conn_elements = session_elem.findall(f'.//SESSIONEXTENSION[@TYPE="{conn_type}"]//CONNECTIONREFERENCE')
        
        for conn_elem in conn_elements:
            conn_name = conn_elem.get('CONNECTIONNAME', '')
            if conn_name:
                connections.append(conn_name)
        
        return connections
    
    def _extract_session_parameters(self, session_elem: ET.Element) -> List[WorkflowParameter]:
        """Extract session-level parameters"""
        
        parameters = []
        param_elements = session_elem.findall('.//SESSIONPARAMETER')
        
        for param_elem in param_elements:
            param = WorkflowParameter(
                parameter_name=param_elem.get('NAME', ''),
                parameter_type=param_elem.get('DATATYPE', 'STRING'),
                default_value=param_elem.get('VALUE', ''),
                is_input=True,
                is_output=False
            )
            parameters.append(param)
        
        return parameters
    
    def _extract_schedule(self, workflow_elem: ET.Element) -> ScheduleInfo:
        """Extract workflow scheduling information"""
        
        scheduler_elem = workflow_elem.find('.//SCHEDULER')
        
        if scheduler_elem is None:
            return ScheduleInfo(
                schedule_name='',
                schedule_type='MANUAL',
                start_time='',
                end_time='',
                recurrence_pattern='NONE',
                days_of_week=[],
                is_enabled=False
            )
        
        schedule_name = scheduler_elem.get('NAME', '')
        schedule_type = scheduler_elem.get('SCHEDULETYPE', 'MANUAL')
        start_time = scheduler_elem.get('STARTTIME', '')
        end_time = scheduler_elem.get('ENDTIME', '')
        recurrence = scheduler_elem.get('RECURRENCE', 'NONE')
        is_enabled = scheduler_elem.get('ISENABLED', 'NO') == 'YES'
        
        days_of_week = []
        if schedule_type == 'WEEKLY':
            days_elem = scheduler_elem.find('.//DAYSOFWEEK')
            if days_elem is not None:
                days_text = days_elem.text or ''
                days_of_week = [day.strip() for day in days_text.split(',') if day.strip()]
        
        return ScheduleInfo(
            schedule_name=schedule_name,
            schedule_type=schedule_type,
            start_time=start_time,
            end_time=end_time,
            recurrence_pattern=recurrence,
            days_of_week=days_of_week,
            is_enabled=is_enabled
        )
    
    def _extract_error_handling(self, workflow_elem: ET.Element) -> List[ErrorHandling]:
        """Extract error handling and recovery mechanisms"""
        
        error_handlers = []
        
        task_elements = workflow_elem.findall('.//TASK')
        for task_elem in task_elements:
            task_name = task_elem.get('NAME', '')
            
            on_error = task_elem.get('ONERROR', 'FAIL')
            recovery = task_elem.get('RECOVERYSTRATEGY', 'RESTART_TASK')
            suspend = task_elem.get('SUSPENDONERROR', 'NO') == 'YES'
            
            notification_elem = task_elem.find('.//EMAILOPTION[@TYPE="FAILURE"]')
            email_notification = notification_elem is not None
            recipients = []
            
            if notification_elem is not None:
                email_to = notification_elem.get('EMAILTO', '')
                recipients = [email.strip() for email in email_to.split(',') if email.strip()]
            
            error_handler = ErrorHandling(
                error_type='TASK_FAILURE',
                on_error_action=on_error,
                recovery_strategy=recovery,
                email_notification=email_notification,
                notification_recipients=recipients,
                suspend_on_error=suspend,
                task_name=task_name
            )
            error_handlers.append(error_handler)
        
        return error_handlers
    
    def _extract_workflow_dependencies(self, workflow_elem: ET.Element) -> List[str]:
        """Extract external workflow dependencies"""
        
        dependencies = []
        
        event_wait_tasks = workflow_elem.findall('.//TASK[@TYPE="Event Wait"]')
        for event_task in event_wait_tasks:
            event_name = event_task.get('EVENTNAME', '')
            if event_name:
                dependencies.append(event_name)
        
        workflow_dependencies = workflow_elem.findall('.//WORKFLOWDEPENDENCY')
        for dep_elem in workflow_dependencies:
            dep_workflow = dep_elem.get('WORKFLOWNAME', '')
            if dep_workflow:
                dependencies.append(dep_workflow)
        
        return dependencies
    
    def _identify_orchestration_pattern(self, tasks: List[WorkflowTask]) -> str:
        """Identify orchestration pattern based on task structure"""
        
        if not tasks:
            return 'EMPTY'
        
        task_count = len(tasks)
        
        parallel_count = sum(1 for task in tasks if not task.task_dependencies)
        if parallel_count > 1:
            return 'PARALLEL'
        
        dependency_counts = [len(task.task_dependencies) for task in tasks]
        max_dependencies = max(dependency_counts) if dependency_counts else 0
        
        if max_dependencies == 0:
            return 'SINGLE_TASK'
        elif max_dependencies == 1:
            return 'SEQUENTIAL'
        else:
            return 'COMPLEX'
    
    def analyze_dependencies(self) -> List[DependencyRelationship]:
        """Analyze and create dependency relationships between workflows"""
        
        print("Analyzing workflow dependencies...")
        
        for workflow_name, workflow_def in self.workflows.items():
            
            for dep_workflow in workflow_def.dependencies:
                relationship = DependencyRelationship(
                    source_workflow=workflow_name,
                    target_workflow=dep_workflow,
                    dependency_type='EVENT_WAIT',
                    condition='',
                    execution_order=0
                )
                self.dependencies.append(relationship)
            
            for task in workflow_def.tasks:
                if task.task_type in ['Event Raise', 'Command']:
                    potential_dep = self._identify_downstream_workflow(task)
                    if potential_dep:
                        relationship = DependencyRelationship(
                            source_workflow=workflow_name,
                            target_workflow=potential_dep,
                            dependency_type='TRIGGER',
                            condition='',
                            execution_order=0
                        )
                        self.dependencies.append(relationship)
        
        self._calculate_execution_order()
        
        print(f"Identified {len(self.dependencies)} dependency relationships")
        return self.dependencies
    
    def _identify_downstream_workflow(self, task: WorkflowTask) -> str:
        """Identify downstream workflow from task description or command"""
        
        for workflow_name in self.workflows.keys():
            if workflow_name.lower() in task.description.lower():
                return workflow_name
            if workflow_name.lower() in task.task_name.lower():
                return workflow_name
        
        return ''
    
    def _calculate_execution_order(self):
        """Calculate execution order for dependent workflows"""
        
        workflow_levels = {}
        
        for workflow_name in self.workflows.keys():
            if workflow_name not in workflow_levels:
                self._calculate_level(workflow_name, workflow_levels, set())
        
        for dep in self.dependencies:
            source_level = workflow_levels.get(dep.source_workflow, 0)
            target_level = workflow_levels.get(dep.target_workflow, 0)
            dep.execution_order = target_level - source_level
    
    def _calculate_level(self, workflow_name: str, levels: Dict[str, int], visited: Set[str]) -> int:
        """Recursively calculate workflow execution level"""
        
        if workflow_name in visited:
            return levels.get(workflow_name, 0)
        
        visited.add(workflow_name)
        
        dependencies = [dep.target_workflow for dep in self.dependencies 
                       if dep.source_workflow == workflow_name]
        
        if not dependencies:
            levels[workflow_name] = 0
            return 0
        
        max_level = 0
        for dep_workflow in dependencies:
            dep_level = self._calculate_level(dep_workflow, levels, visited)
            max_level = max(max_level, dep_level + 1)
        
        levels[workflow_name] = max_level
        return max_level
    
    def create_dependency_diagrams(self):
        """Create visual dependency diagrams using Graphviz"""
        
        print("Creating dependency diagrams...")
        
        self._create_overall_dependency_diagram()
        
        for workflow_name in self.workflows.keys():
            self._create_workflow_task_diagram(workflow_name)
        
        print(f"Dependency diagrams saved to {self.output_directory}")
    
    def _create_overall_dependency_diagram(self):
        """Create overall workflow dependency diagram"""
        
        dot = graphviz.Digraph(comment='Workflow Dependencies')
        dot.attr(rankdir='TB', size='16,12')
        dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightblue')
        
        for workflow_name, workflow_def in self.workflows.items():
            label = f"{workflow_name}\\n{workflow_def.orchestration_pattern}"
            color = 'lightgreen' if workflow_def.is_enabled else 'lightgray'
            dot.node(workflow_name, label, fillcolor=color)
        
        for dep in self.dependencies:
            label = f"{dep.dependency_type}"
            dot.edge(dep.source_workflow, dep.target_workflow, label=label)
        
        output_path = os.path.join(self.output_directory, 'workflow_dependencies')
        dot.render(output_path, format='png', cleanup=True)
    
    def _create_workflow_task_diagram(self, workflow_name: str):
        """Create task-level diagram for individual workflow"""
        
        if workflow_name not in self.workflows:
            return
        
        workflow_def = self.workflows[workflow_name]
        
        dot = graphviz.Digraph(comment=f'Workflow: {workflow_name}')
        dot.attr(rankdir='TB', size='12,16')
        dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightyellow')
        
        for task in workflow_def.tasks:
            label = f"{task.task_name}\\n({task.task_type})"
            color = self._get_task_color(task.task_type)
            dot.node(task.task_name, label, fillcolor=color)
        
        for task in workflow_def.tasks:
            for dep_task in task.task_dependencies:
                dot.edge(dep_task, task.task_name)
        
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', workflow_name)
        output_path = os.path.join(self.output_directory, f'workflow_{safe_name}')
        dot.render(output_path, format='png', cleanup=True)
    
    def _get_task_color(self, task_type: str) -> str:
        """Get color coding for different task types"""
        
        color_map = {
            'SESSION': 'lightgreen',
            'Command': 'lightcoral',
            'Event Wait': 'lightblue',
            'Event Raise': 'lightyellow',
            'Decision': 'orange',
            'Timer': 'pink',
            'Email': 'violet',
            'Start': 'lightgray',
            'Assignment': 'wheat'
        }
        
        return color_map.get(task_type, 'white')
    
    def export_to_json(self):
        """Export workflow analysis to JSON format"""
        
        print("Exporting analysis to JSON...")
        
        workflows_data = {
            name: self._workflow_to_dict(workflow) 
            for name, workflow in self.workflows.items()
        }
        
        dependencies_data = [
            asdict(dep) for dep in self.dependencies
        ]
        
        analysis_report = {
            'analysis_timestamp': datetime.now().isoformat(),
            'total_workflows': len(self.workflows),
            'total_dependencies': len(self.dependencies),
            'workflows': workflows_data,
            'dependencies': dependencies_data
        }
        
        json_path = os.path.join(self.output_directory, 'workflow_analysis.json')
        with open(json_path, 'w') as f:
            json.dump(analysis_report, f, indent=2, default=str)
        
        print(f"JSON export saved to {json_path}")
    
    def _workflow_to_dict(self, workflow: WorkflowDefinition) -> Dict:
        """Convert workflow definition to dictionary"""
        
        return {
            'workflow_name': workflow.workflow_name,
            'workflow_version': workflow.workflow_version,
            'folder_name': workflow.folder_name,
            'description': workflow.description,
            'is_valid': workflow.is_valid,
            'is_enabled': workflow.is_enabled,
            'start_task': workflow.start_task,
            'orchestration_pattern': workflow.orchestration_pattern,
            'tasks': [asdict(task) for task in workflow.tasks],
            'parameters': [asdict(param) for param in workflow.parameters],
            'variables': [asdict(var) for var in workflow.variables],
            'connections': [asdict(conn) for conn in workflow.connections],
            'sessions': [asdict(session) for session in workflow.sessions],
            'schedule': asdict(workflow.schedule),
            'error_handling': [asdict(eh) for eh in workflow.error_handling],
            'dependencies': workflow.dependencies
        }
    
    def export_to_spark_dataframes(self):
        """Export analysis results to Spark DataFrames and Parquet"""
        
        print("Exporting to Spark DataFrames...")
        
        workflows_df = self._create_workflows_dataframe()
        tasks_df = self._create_tasks_dataframe()
        dependencies_df = self._create_dependencies_dataframe()
        parameters_df = self._create_parameters_dataframe()
        connections_df = self._create_connections_dataframe()
        schedules_df = self._create_schedules_dataframe()
        
        parquet_dir = os.path.join(self.output_directory, 'parquet')
        os.makedirs(parquet_dir, exist_ok=True)
        
        workflows_df.write.mode('overwrite').parquet(
            os.path.join(parquet_dir, 'workflows')
        )
        tasks_df.write.mode('overwrite').parquet(
            os.path.join(parquet_dir, 'tasks')
        )
        dependencies_df.write.mode('overwrite').parquet(
            os.path.join(parquet_dir, 'dependencies')
        )
        parameters_df.write.mode('overwrite').parquet(
            os.path.join(parquet_dir, 'parameters')
        )
        connections_df.write.mode('overwrite').parquet(
            os.path.join(parquet_dir, 'connections')
        )
        schedules_df.write.mode('overwrite').parquet(
            os.path.join(parquet_dir, 'schedules')
        )
        
        print(f"Parquet files saved to {parquet_dir}")
        
        return {
            'workflows': workflows_df,
            'tasks': tasks_df,
            'dependencies': dependencies_df,
            '