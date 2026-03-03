import os
import sys
import json
import xml.etree.ElementTree as ET
from typing import Dict, List, Set, Tuple, Any, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime
from collections import defaultdict, deque
import logging
from pathlib import Path
import graphviz
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType
from pyspark.sql.functions import col, explode, collect_list, struct, current_timestamp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ConnectionInfo:
    """Data class for connection details"""
    connection_name: str
    connection_type: str
    username: str = ""
    database_name: str = ""
    server_name: str = ""
    connection_string: str = ""
    attributes: Dict[str, str] = field(default_factory=dict)


@dataclass
class SessionConfig:
    """Data class for session configuration"""
    session_name: str
    mapping_name: str
    source_connections: List[str] = field(default_factory=list)
    target_connections: List[str] = field(default_factory=list)
    session_properties: Dict[str, str] = field(default_factory=dict)
    performance_settings: Dict[str, str] = field(default_factory=dict)
    error_handling: Dict[str, str] = field(default_factory=dict)
    recovery_strategy: str = ""


@dataclass
class WorkflowTask:
    """Data class for workflow task"""
    task_name: str
    task_type: str
    task_instance_name: str = ""
    is_enabled: bool = True
    fail_parent_on_failure: bool = True
    treat_input_links_as: str = "AND"
    predecessor_tasks: List[str] = field(default_factory=list)
    successor_tasks: List[str] = field(default_factory=list)
    session_config: Optional[SessionConfig] = None
    command: str = ""
    decision_logic: str = ""
    task_properties: Dict[str, str] = field(default_factory=dict)


@dataclass
class ScheduleInfo:
    """Data class for scheduling information"""
    schedule_name: str
    schedule_type: str  # Time-based, Event-based, Manual
    frequency: str = ""
    start_time: str = ""
    end_time: str = ""
    days_of_week: List[str] = field(default_factory=list)
    days_of_month: List[int] = field(default_factory=list)
    trigger_event: str = ""
    scheduler_properties: Dict[str, str] = field(default_factory=dict)


@dataclass
class WorkflowVariable:
    """Data class for workflow variables"""
    variable_name: str
    variable_type: str
    default_value: str = ""
    is_persistent: bool = False
    is_system_defined: bool = False
    description: str = ""


@dataclass
class WorkflowParameter:
    """Data class for workflow parameters"""
    parameter_name: str
    parameter_type: str
    default_value: str = ""
    is_required: bool = False
    description: str = ""


@dataclass
class ErrorHandlingConfig:
    """Data class for error handling configuration"""
    on_session_failure: str = ""
    on_task_failure: str = ""
    recovery_strategy: str = ""
    max_retry_attempts: int = 0
    retry_delay: int = 0
    suspend_on_error: bool = False
    email_on_failure: bool = False
    email_addresses: List[str] = field(default_factory=list)
    error_log_path: str = ""


@dataclass
class WorkflowDefinition:
    """Data class for complete workflow definition"""
    workflow_name: str
    workflow_description: str = ""
    folder_name: str = ""
    repository_name: str = ""
    version: str = ""
    is_enabled: bool = True
    is_valid: bool = True
    run_mode: str = "Normal"
    tasks: List[WorkflowTask] = field(default_factory=list)
    task_dependencies: Dict[str, List[str]] = field(default_factory=dict)
    schedule_info: Optional[ScheduleInfo] = None
    variables: List[WorkflowVariable] = field(default_factory=list)
    parameters: List[WorkflowParameter] = field(default_factory=list)
    error_handling: ErrorHandlingConfig = field(default_factory=ErrorHandlingConfig)
    workflow_properties: Dict[str, str] = field(default_factory=dict)
    execution_order: List[str] = field(default_factory=list)
    parallel_execution_groups: List[List[str]] = field(default_factory=list)


@dataclass
class WorkflowDependency:
    """Data class for workflow-level dependencies"""
    workflow_name: str
    depends_on_workflows: List[str] = field(default_factory=list)
    dependency_type: str = ""  # Sequential, Conditional, Event-based
    dependency_condition: str = ""


class PowerCenterXMLParser:
    """Parser for PowerCenter XML files"""
    
    NAMESPACE = {
        'pc': 'http://www.informatica.com/powercenter',
        'pmrep': 'http://www.informatica.com/pmrep'
    }
    
    def __init__(self):
        self.workflows: Dict[str, WorkflowDefinition] = {}
        self.connections: Dict[str, ConnectionInfo] = {}
        self.sessions: Dict[str, SessionConfig] = {}
        self.workflow_dependencies: Dict[str, WorkflowDependency] = {}
        
    def parse_xml_file(self, xml_file_path: str) -> None:
        """Parse a single PowerCenter XML file"""
        try:
            logger.info(f"Parsing XML file: {xml_file_path}")
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            
            # Parse different object types
            self._parse_connections(root)
            self._parse_sessions(root)
            self._parse_workflows(root)
            
            logger.info(f"Successfully parsed: {xml_file_path}")
            
        except ET.ParseError as e:
            logger.error(f"XML parsing error in {xml_file_path}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error parsing {xml_file_path}: {str(e)}")
            raise
    
    def _parse_connections(self, root: ET.Element) -> None:
        """Parse connection objects from XML"""
        # Handle different XML structures for connections
        connection_elements = (
            root.findall('.//CONNECTION') +
            root.findall('.//RELATIONALCONNECTION') +
            root.findall('.//APPLICATION_CONNECTION')
        )
        
        for conn in connection_elements:
            conn_name = conn.get('NAME', '')
            conn_type = conn.get('TYPE', conn.get('CONNECTIONTYPE', ''))
            
            connection = ConnectionInfo(
                connection_name=conn_name,
                connection_type=conn_type,
                username=conn.get('USERNAME', ''),
                database_name=conn.get('DATABASENAME', ''),
                server_name=conn.get('SERVERNAME', ''),
                connection_string=conn.get('CONNECTIONSTRING', ''),
                attributes={attr.tag: attr.text for attr in conn if attr.text}
            )
            
            self.connections[conn_name] = connection
            logger.debug(f"Parsed connection: {conn_name}")
    
    def _parse_sessions(self, root: ET.Element) -> None:
        """Parse session objects from XML"""
        session_elements = root.findall('.//SESSION')
        
        for session in session_elements:
            session_name = session.get('NAME', '')
            mapping_name = session.get('MAPPINGNAME', '')
            
            # Parse session properties
            session_props = {}
            config_ref = session.find('.//CONFIGREFERENCE')
            if config_ref is not None:
                session_props = {attr: config_ref.get(attr) 
                               for attr in config_ref.attrib}
            
            # Parse source and target connections
            source_conns = []
            target_conns = []
            
            for sesstrans in session.findall('.//SESSTRANSFORMATIONINST'):
                conn_info = sesstrans.find('.//CONNECTIONREFERENCE')
                if conn_info is not None:
                    conn_name = conn_info.get('CONNECTIONNAME', '')
                    trans_type = sesstrans.get('TRANSFORMATIONTYPE', '')
                    
                    if trans_type in ['Source Qualifier', 'Source']:
                        source_conns.append(conn_name)
                    elif trans_type in ['Target', 'Target Definition']:
                        target_conns.append(conn_name)
            
            # Parse error handling
            error_handling = {}
            for attr in session.findall('.//ATTRIBUTE'):
                attr_name = attr.get('NAME', '')
                attr_value = attr.get('VALUE', '')
                if any(err_key in attr_name.upper() for err_key in 
                      ['ERROR', 'RECOVERY', 'RETRY', 'FAIL']):
                    error_handling[attr_name] = attr_value
            
            session_config = SessionConfig(
                session_name=session_name,
                mapping_name=mapping_name,
                source_connections=source_conns,
                target_connections=target_conns,
                session_properties=session_props,
                error_handling=error_handling
            )
            
            self.sessions[session_name] = session_config
            logger.debug(f"Parsed session: {session_name}")
    
    def _parse_workflows(self, root: ET.Element) -> None:
        """Parse workflow objects from XML"""
        workflow_elements = root.findall('.//WORKFLOW')
        
        for wf in workflow_elements:
            workflow_name = wf.get('NAME', '')
            
            workflow = WorkflowDefinition(
                workflow_name=workflow_name,
                workflow_description=wf.get('DESCRIPTION', ''),
                folder_name=wf.get('FOLDERNAME', ''),
                repository_name=wf.get('REPOSITORYNAME', ''),
                version=wf.get('VERSION', ''),
                is_enabled=wf.get('ISENABLED', 'YES') == 'YES',
                is_valid=wf.get('ISVALID', 'YES') == 'YES'
            )
            
            # Parse workflow properties
            for attr in wf.findall('.//ATTRIBUTE'):
                attr_name = attr.get('NAME', '')
                attr_value = attr.get('VALUE', '')
                workflow.workflow_properties[attr_name] = attr_value
            
            # Parse scheduler information
            scheduler = wf.find('.//SCHEDULER')
            if scheduler is not None:
                workflow.schedule_info = self._parse_scheduler(scheduler)
            
            # Parse workflow variables
            for var in wf.findall('.//WORKFLOWVARIABLE'):
                workflow.variables.append(WorkflowVariable(
                    variable_name=var.get('NAME', ''),
                    variable_type=var.get('DATATYPE', ''),
                    default_value=var.get('DEFAULTVALUE', ''),
                    is_persistent=var.get('ISPERSISTENT', 'NO') == 'YES',
                    is_system_defined=var.get('ISSYSTEMDEFINED', 'NO') == 'YES'
                ))
            
            # Parse workflow parameters
            for param in wf.findall('.//WORKFLOWPARAMETER'):
                workflow.parameters.append(WorkflowParameter(
                    parameter_name=param.get('NAME', ''),
                    parameter_type=param.get('DATATYPE', ''),
                    default_value=param.get('DEFAULTVALUE', ''),
                    is_required=param.get('ISREQUIRED', 'NO') == 'YES'
                ))
            
            # Parse tasks
            workflow.tasks = self._parse_workflow_tasks(wf)
            
            # Build task dependencies
            workflow.task_dependencies = self._build_task_dependencies(workflow.tasks)
            
            # Determine execution order
            workflow.execution_order = self._determine_execution_order(workflow.tasks)
            
            # Identify parallel execution groups
            workflow.parallel_execution_groups = self._identify_parallel_groups(workflow.tasks)
            
            # Parse error handling configuration
            workflow.error_handling = self._parse_error_handling(wf)
            
            self.workflows[workflow_name] = workflow
            logger.info(f"Parsed workflow: {workflow_name} with {len(workflow.tasks)} tasks")
    
    def _parse_scheduler(self, scheduler: ET.Element) -> ScheduleInfo:
        """Parse scheduler configuration"""
        schedule_type = scheduler.get('SCHEDULETYPE', 'Manual')
        
        schedule = ScheduleInfo(
            schedule_name=scheduler.get('NAME', ''),
            schedule_type=schedule_type,
            frequency=scheduler.get('FREQUENCY', ''),
            start_time=scheduler.get('STARTTIME', ''),
            end_time=scheduler.get('ENDTIME', '')
        )
        
        # Parse schedule properties
        for attr in scheduler.findall('.//ATTRIBUTE'):
            attr_name = attr.get('NAME', '')
            attr_value = attr.get('VALUE', '')
            schedule.scheduler_properties[attr_name] = attr_value
            
            # Extract specific schedule information
            if 'DAYOFWEEK' in attr_name.upper():
                schedule.days_of_week.append(attr_value)
            elif 'DAYOFMONTH' in attr_name.upper():
                try:
                    schedule.days_of_month.append(int(attr_value))
                except ValueError:
                    pass
        
        return schedule
    
    def _parse_workflow_tasks(self, workflow: ET.Element) -> List[WorkflowTask]:
        """Parse all tasks in a workflow"""
        tasks = []
        
        for task_elem in workflow.findall('.//TASK'):
            task_name = task_elem.get('NAME', '')
            task_type = task_elem.get('TYPE', task_elem.get('TASKTYPE', ''))
            
            task = WorkflowTask(
                task_name=task_name,
                task_type=task_type,
                task_instance_name=task_elem.get('INSTANCENAME', task_name),
                is_enabled=task_elem.get('ISENABLED', 'YES') == 'YES',
                fail_parent_on_failure=task_elem.get('FAILPARENTIFFAILS', 'YES') == 'YES',
                treat_input_links_as=task_elem.get('TREATINPUTLINKSAS', 'AND')
            )
            
            # Parse task properties
            for attr in task_elem.findall('.//ATTRIBUTE'):
                attr_name = attr.get('NAME', '')
                attr_value = attr.get('VALUE', '')
                task.task_properties[attr_name] = attr_value
            
            # Link to session configuration if applicable
            if task_type in ['Session', 'Mapping']:
                ref_obj = task_elem.get('REFOBJECTNAME', '')
                if ref_obj in self.sessions:
                    task.session_config = self.sessions[ref_obj]
            
            # Parse command task details
            if task_type == 'Command':
                task.command = task_elem.get('COMMAND', '')
            
            # Parse decision task logic
            if task_type == 'Decision':
                decision = task_elem.find('.//DECISION')
                if decision is not None:
                    task.decision_logic = decision.get('CONDITION', '')
            
            tasks.append(task)
            logger.debug(f"Parsed task: {task_name} of type {task_type}")
        
        # Parse task links to establish dependencies
        self._parse_task_links(workflow, tasks)
        
        return tasks
    
    def _parse_task_links(self, workflow: ET.Element, tasks: List[WorkflowTask]) -> None:
        """Parse task links to establish task dependencies"""
        task_map = {task.task_name: task for task in tasks}
        
        for link in workflow.findall('.//TASKLINK'):
            from_task = link.get('FROMTASK', '')
            to_task = link.get('TOTASK', '')
            link_condition = link.get('CONDITION', '')
            
            if from_task in task_map and to_task in task_map:
                task_map[from_task].successor_tasks.append(to_task)
                task_map[to_task].predecessor_tasks.append(from_task)
                
                # Store link condition in task properties
                if link_condition:
                    task_map[to_task].task_properties[f'link_condition_from_{from_task}'] = link_condition
    
    def _build_task_dependencies(self, tasks: List[WorkflowTask]) -> Dict[str, List[str]]:
        """Build task dependency dictionary"""
        dependencies = {}
        for task in tasks:
            dependencies[task.task_name] = task.predecessor_tasks
        return dependencies
    
    def _determine_execution_order(self, tasks: List[WorkflowTask]) -> List[str]:
        """Determine task execution order using topological sort"""
        # Build adjacency list
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        
        all_tasks = {task.task_name for task in tasks}
        
        for task in tasks:
            if task.task_name not in in_degree:
                in_degree[task.task_name] = 0
            
            for successor in task.successor_tasks:
                graph[task.task_name].append(successor)
                in_degree[successor] += 1
        
        # Topological sort using Kahn's algorithm
        queue = deque([task for task in all_tasks if in_degree[task] == 0])
        execution_order = []
        
        while queue:
            current = queue.popleft()
            execution_order.append(current)
            
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        # Check for cycles
        if len(execution_order) != len(all_tasks):
            logger.warning("Cycle detected in workflow tasks - execution order may be incomplete")
        
        return execution_order
    
    def _identify_parallel_groups(self, tasks: List[WorkflowTask]) -> List[List[str]]:
        """Identify tasks that can be executed in parallel"""
        task_map = {task.task_name: task for task in tasks}
        parallel_groups = []
        processed = set()
        
        # Group tasks by their predecessor count
        execution_levels = defaultdict(list)
        
        for task in tasks:
            level = len(task.predecessor_tasks)
            execution_levels[level].append(task.task_name)
        
        # Tasks at the same level with no dependencies on each other can run in parallel
        for level in sorted(execution_levels.keys()):
            level_tasks = execution_levels[level]
            
            # Check if tasks can truly run in parallel
            if len(level_tasks) > 1:
                # Verify no inter-dependencies within the level
                can_parallelize = True
                for i, task1 in enumerate(level_tasks):
                    for task2 in level_tasks[i+1:]:
                        if (task2 in task_map[task1].successor_tasks or 
                            task1 in task_map[task2].successor_tasks):
                            can_parallelize = False
                            break
                    if not can_parallelize:
                        break
                
                if can_parallelize:
                    parallel_groups.append(level_tasks)
        
        return parallel_groups
    
    def _parse_error_handling(self, workflow: ET.Element) -> ErrorHandlingConfig:
        """Parse error handling configuration"""
        error_config = ErrorHandlingConfig()
        
        for attr in workflow.findall('.//ATTRIBUTE'):
            attr_name = attr.get('NAME', '').upper()
            attr_value = attr.get('VALUE', '')
            
            if 'ONSESSIONFAILURE' in attr_name:
                error_config.on_session_failure = attr_value
            elif 'ONTASKFAILURE' in attr_name:
                error_config.on_task_failure = attr_value
            elif 'RECOVERY' in attr_name:
                error_config.recovery_strategy = attr_value
            elif 'RETRY' in attr_name and 'COUNT' in attr_name:
                try:
                    error_config.max_retry_attempts = int(attr_value)
                except ValueError:
                    pass
            elif 'RETRY' in attr_name and 'DELAY' in attr_name:
                try:
                    error_config.retry_delay = int(attr_value)
                except ValueError:
                    pass
            elif 'SUSPEND' in attr_name and 'ERROR' in attr_name:
                error_config.suspend_on_error = attr_value.upper() in ['YES', 'TRUE', '1']
            elif 'EMAIL' in attr_name and 'FAILURE' in attr_name:
                error_config.email_on_failure = attr_value.upper() in ['YES', 'TRUE', '1']
            elif 'EMAIL' in attr_name and 'ADDRESS' in attr_name:
                error_config.email_addresses = [addr.strip() for addr in attr_value.split(',')]
            elif 'ERRORLOG' in attr_name:
                error_config.error_log_path = attr_value
        
        return error_config
    
    def analyze_workflow_dependencies(self) -> None:
        """Analyze dependencies between workflows"""
        for wf_name, workflow in self.workflows.items():
            dependency = WorkflowDependency(workflow_name=wf_name)
            
            # Check for workflow-level dependencies
            for task in workflow.tasks:
                # Check if task triggers another workflow
                if task.task_type in ['Workflow', 'Worklet']:
                    ref_workflow = task.task_properties.get('REFOBJECTNAME', '')
                    if ref_workflow and ref_workflow in self.workflows:
                        dependency.depends_on_workflows.append(ref_workflow)
                
                # Check for event-based dependencies
                if 'EVENT' in task.task_type.upper():
                    event_name = task.task_properties.get('EVENTNAME', '')
                    # Find workflows that raise this event
                    for other_wf_name, other_wf in self.workflows.items():
                        if other_wf_name != wf_name:
                            for other_task in other_wf.tasks:
                                if (other_task.task_type == 'Event Raise' and 
                                    other_task.task_properties.get('EVENTNAME', '') == event_name):
                                    dependency.depends_on_workflows.append(other_wf_name)
                                    dependency.dependency_type = 'Event-based'
            
            if dependency.depends_on_workflows:
                self.workflow_dependencies[wf_name] = dependency


class WorkflowAnalyzer:
    """Analyzer for workflow patterns and dependencies"""
    
    def __init__(self, parser: PowerCenterXMLParser):
        self.parser = parser
        self.analysis_results = {}
    
    def analyze_all(self) -> Dict[str, Any]:
        """Perform comprehensive workflow analysis"""
        logger.info("Starting comprehensive workflow analysis...")
        
        self.analysis_results = {
            'workflow_catalog': self._create_workflow_catalog(),
            'dependency_tree': self._create_dependency_tree(),
            'scheduling_summary': self._analyze_scheduling(),
            'parameter_usage': self._analyze_parameter_usage(),
            'connection_requirements': self._analyze_connections(),
            'error_handling_patterns': self._analyze_error_handling(),
            'orchestration_patterns': self._identify_orchestration_patterns(),
            'complexity_metrics': self._calculate_complexity_metrics()
        }
        
        logger.info("Workflow analysis completed")
        return self.analysis_results
    
    def _create_workflow_catalog(self) -> List[Dict[str, Any]]:
        """Create comprehensive workflow catalog"""
        catalog = []
        
        for wf_name, workflow in self.parser.workflows.items():
            catalog_entry = {
                'workflow_name': wf_name,
                'description': workflow.workflow_description,
                'folder': workflow.folder_name,
                'is_enabled': workflow.is_enabled,
                'is_valid': workflow.is_valid,
                'task_count': len(workflow.tasks),
                'execution_sequence': workflow.execution_order,
                'has_schedule': workflow.schedule_info is not None,
                'schedule_type': workflow.schedule_info.schedule_type if workflow.schedule_info else 'Manual',
                'parameter_count': len(workflow.parameters),
                'variable_count': len(workflow.variables),
                'parallel_groups_count': len(workflow.parallel_execution_groups),
                'tasks': [
                    {
                        'task_name': task.task_name,
                        'task_type': task.task_type,
                        'is_enabled': task.is_enabled,
                        'predecessor_count': len(task.predecessor_tasks),
                        'successor_count': len(task.successor_tasks)
                    }
                    for task in workflow.tasks
                ]
            }
            catalog.append(catalog_entry)
        
        return catalog
    
    def _create_dependency_tree(self) -> Dict[str, Any]:
        """Create workflow dependency tree"""
        dependency_tree = {
            'workflows': {},
            'dependency_chains': [],
            'circular_dependencies': []
        }
        
        for wf_name, dependency in self.parser.workflow_dependencies.items():
            dependency_tree['workflows'][wf_name] = {
                'depends_on': dependency.depends_on_workflows,
                'dependency_type': dependency.dependency_type,
                'depth': self._calculate_dependency_depth(wf_name)
            }
        
        # Find dependency chains
        dependency_tree['dependency_chains'] = self._find_dependency_chains()
        
        # Detect circular dependencies
        dependency_tree['circular_dependencies'] = self._detect_circular_dependencies()
        
        return dependency_tree
    
    def _calculate_dependency_depth(self, workflow_name: str, visited: Set[str] = None) -> int:
        """Calculate the depth of workflow dependencies"""
        if visited is None:
            visited = set()
        
        if workflow_name in visited:
            return 0
        
        visited.add(workflow_name)
        
        if workflow_name not in self.parser.workflow_dependencies:
            return 0
        
        dependency = self.parser.workflow_dependencies[workflow_name]
        if not dependency.depends_on_workflows:
            return 0
        
        max_depth = 0
        for dep_wf in dependency.depends_on_workflows:
            depth = self._calculate_dependency_depth(dep_wf, visited.copy())
            max_depth = max(max_depth, depth)
        
        return max_depth + 1
    
    def _find_dependency_chains(self) -> List[List[str]]:
        """Find all dependency chains in workflows"""
        chains = []
        
        # Find root workflows (no dependencies)
        roots = [wf for wf in self.parser.workflows.keys() 
                if wf not in self.parser.workflow_dependencies or 
                not self.parser.workflow_dependencies[wf].depends_on_workflows]
        
        # Build chains from each root
        for root in roots:
            self._build_chain(root, [root], chains)
        
        return chains
    
    def _build_chain(self, current: str, path: List[str], chains: List[List[str]]) -> None:
        """Recursively build dependency chain"""
        has_dependents = False
        
        for wf_name, dependency in self.parser.workflow_dependencies.items():
            if current in dependency.depends_on_workflows and wf_name not in path:
                has_dependents = True
                self._build_chain(wf_name, path + [wf_name], chains)
        
        if not has_dependents and len(path) > 1:
            chains.append(path)
    
    def _detect_circular_dependencies(self) -> List[List[str]]:
        """Detect circular dependencies between workflows"""
        circular_deps = []
        visited = set()
        rec_stack = set()
        
        def dfs(workflow: str, path: List[str]) -> None:
            visited.add(workflow)
            rec_stack.add(workflow)
            path.append(workflow)
            
            if workflow in self.parser.workflow_dependencies:
                for dep_wf in self.parser.workflow_dependencies[workflow].depends_on_workflows:
                    if dep_wf not in visited:
                        dfs(dep_wf, path.copy())
                    elif dep_wf in rec_stack:
                        # Found a cycle
                        cycle_start = path.index(dep_wf)
                        cycle = path[cycle_start:] + [dep_wf]
                        if cycle not in circular_deps:
                            circular_deps.append(cycle)
            
            rec_stack.remove(workflow)
        
        for workflow in self.parser.workflows.keys():
            if workflow not in visited:
                dfs(workflow, [])
        
        return circular_deps
    
    def _analyze_scheduling(self) -> Dict[str, Any]:
        """Analyze scheduling patterns across workflows"""
        scheduling_summary = {
            'scheduled_workflows': 0,
            'manual_workflows': 0,
            'event_based_workflows': 0,
            'schedule_types': defaultdict(int),
            'schedules': []
        }
        
        for wf_name, workflow in self.parser.workflows.items():
            if workflow.schedule_info:
                scheduling_summary['scheduled_workflows'] += 1
                schedule_type = workflow.schedule_info.schedule_type
                scheduling_summary['schedule_types'][schedule_type] += 1
                
                scheduling_summary['schedules'].append({
                    'workflow_name': wf_name,
                    'schedule_type': schedule_type,
                    'frequency': workflow.schedule_info.frequency,
                    'start_time': workflow.schedule_info.start_time,
                    'days_of_week': workflow.schedule_info.days_of_week,
                    'days_of_month': workflow.schedule_info.days_of_month
                })
            else:
                scheduling_summary['manual_workflows'] += 1
        
        # Convert defaultdict to regular dict for JSON serialization
        scheduling_summary['schedule_types'] = dict(scheduling_summary['schedule_types'])
        
        return scheduling_summary
    
    def _analyze_parameter_usage(self) -> Dict[str, Any]:
        """Analyze parameter and variable usage across workflows"""
        parameter_usage = {
            'workflows_with_parameters': 0,
            'workflows_with_variables': 0,
            'total_parameters': 0,
            'total_variables': 0,
            'parameter_details': [],
            'variable_details': [],
            'shared_parameters': defaultdict(list)
        }
        
        for wf_name, workflow in self.parser.workflows.items():
            if workflow.parameters:
                parameter_usage['workflows_with_parameters'] += 1
                parameter_usage['total_parameters'] += len(workflow.parameters)
                
                for param in workflow.parameters:
                    parameter_usage['parameter_details'].append({
                        'workflow_name': wf_name,
                        'parameter_name':