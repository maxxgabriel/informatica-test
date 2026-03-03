# project_plan_resource_allocation.py
"""
Informatica to PySpark Migration - Project Planning and Resource Allocation Framework
=====================================================================================

This module provides a comprehensive framework for managing the migration project,
including work breakdown structure, resource allocation, timeline management,
and success metrics tracking.

Author: Data Engineering Team
Date: 2024
Version: 1.0
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, date_add, datediff, sum as _sum,
    count, avg, max as _max, min as _min, when, concat_ws, array,
    explode, struct, to_json, from_json, collect_list, row_number,
    dense_rank, percent_rank, lag, lead, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, TimestampType, ArrayType, MapType, BooleanType, DecimalType
)
from pyspark.sql.window import Window
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from enum import Enum
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProjectPhase(Enum):
    """Project phases for migration lifecycle"""
    PLANNING = "Planning"
    ASSESSMENT = "Assessment"
    DESIGN = "Design"
    DEVELOPMENT = "Development"
    TESTING = "Testing"
    DEPLOYMENT = "Deployment"
    HYPERCARE = "Hypercare"
    CLOSURE = "Closure"


class ResourceRole(Enum):
    """Resource roles for project team"""
    PROJECT_MANAGER = "Project Manager"
    ARCHITECT = "Solution Architect"
    LEAD_ENGINEER = "Lead Data Engineer"
    SENIOR_ENGINEER = "Senior Data Engineer"
    DATA_ENGINEER = "Data Engineer"
    QA_LEAD = "QA Lead"
    QA_ENGINEER = "QA Engineer"
    DEVOPS_ENGINEER = "DevOps Engineer"
    BA = "Business Analyst"
    SME = "Subject Matter Expert"


class TaskStatus(Enum):
    """Task status enumeration"""
    NOT_STARTED = "Not Started"
    IN_PROGRESS = "In Progress"
    BLOCKED = "Blocked"
    COMPLETED = "Completed"
    CANCELLED = "Cancelled"


class Priority(Enum):
    """Task priority levels"""
    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4


@dataclass
class Task:
    """Task definition with dependencies and resource allocation"""
    task_id: str
    task_name: str
    description: str
    phase: ProjectPhase
    estimated_hours: float
    start_date: datetime
    end_date: datetime
    dependencies: List[str] = field(default_factory=list)
    assigned_resources: List[str] = field(default_factory=list)
    status: TaskStatus = TaskStatus.NOT_STARTED
    priority: Priority = Priority.MEDIUM
    actual_hours: float = 0.0
    completion_percentage: float = 0.0
    deliverables: List[str] = field(default_factory=list)
    risks: List[str] = field(default_factory=list)


@dataclass
class Resource:
    """Resource definition with allocation and capacity"""
    resource_id: str
    name: str
    role: ResourceRole
    email: str
    hourly_rate: float
    availability_percentage: float
    skills: List[str] = field(default_factory=list)
    start_date: datetime = None
    end_date: datetime = None
    allocated_hours: float = 0.0
    actual_hours: float = 0.0


@dataclass
class SuccessMetric:
    """Success metrics and KPIs for project tracking"""
    metric_id: str
    metric_name: str
    description: str
    target_value: float
    actual_value: float
    unit: str
    measurement_frequency: str
    baseline_value: float = 0.0
    threshold_warning: float = 0.0
    threshold_critical: float = 0.0


@dataclass
class Risk:
    """Risk definition and mitigation plan"""
    risk_id: str
    risk_name: str
    description: str
    probability: str  # High, Medium, Low
    impact: str  # High, Medium, Low
    mitigation_plan: str
    contingency_plan: str
    owner: str
    status: str


class ProjectPlanManager:
    """
    Comprehensive project plan manager for Informatica to PySpark migration
    
    This class manages the complete project lifecycle including:
    - Work breakdown structure
    - Resource allocation and capacity planning
    - Timeline and dependency management
    - Success metrics tracking
    - Risk and issue management
    """
    
    def __init__(self, spark: SparkSession, project_name: str, project_id: str):
        """
        Initialize project plan manager
        
        Args:
            spark: SparkSession instance
            project_name: Name of the migration project
            project_id: Unique project identifier
        """
        self.spark = spark
        self.project_name = project_name
        self.project_id = project_id
        self.tasks: List[Task] = []
        self.resources: List[Resource] = []
        self.metrics: List[SuccessMetric] = []
        self.risks: List[Risk] = []
        
        logger.info(f"Initialized ProjectPlanManager for {project_name} (ID: {project_id})")
    
    def create_work_breakdown_structure(self) -> DataFrame:
        """
        Create comprehensive work breakdown structure (WBS) for migration
        
        Returns:
            DataFrame containing complete WBS with all tasks and phases
        """
        logger.info("Creating work breakdown structure")
        
        # Define comprehensive WBS for Informatica to PySpark migration
        wbs_data = [
            # Phase 1: Planning and Assessment
            {
                "wbs_id": "1.0", "phase": "Planning", "task_name": "Project Initiation",
                "description": "Establish project governance and obtain approvals",
                "estimated_days": 10, "dependencies": [], "resources_required": 3,
                "deliverables": "Project Charter, Stakeholder Register, Communication Plan"
            },
            {
                "wbs_id": "1.1", "phase": "Planning", "task_name": "Stakeholder Analysis",
                "description": "Identify and analyze all project stakeholders",
                "estimated_days": 5, "dependencies": ["1.0"], "resources_required": 2,
                "deliverables": "Stakeholder Matrix, RACI Chart"
            },
            {
                "wbs_id": "1.2", "phase": "Planning", "task_name": "Resource Planning",
                "description": "Define resource requirements and allocation",
                "estimated_days": 5, "dependencies": ["1.0"], "resources_required": 2,
                "deliverables": "Resource Plan, Capacity Model"
            },
            {
                "wbs_id": "2.0", "phase": "Assessment", "task_name": "Current State Assessment",
                "description": "Analyze existing Informatica environment",
                "estimated_days": 15, "dependencies": ["1.0"], "resources_required": 5,
                "deliverables": "Assessment Report, Inventory Catalog"
            },
            {
                "wbs_id": "2.1", "phase": "Assessment", "task_name": "Workflow Inventory",
                "description": "Document all Informatica workflows and mappings",
                "estimated_days": 10, "dependencies": ["2.0"], "resources_required": 4,
                "deliverables": "Workflow Inventory, Complexity Matrix"
            },
            {
                "wbs_id": "2.2", "phase": "Assessment", "task_name": "Dependency Mapping",
                "description": "Map dependencies between workflows and systems",
                "estimated_days": 8, "dependencies": ["2.1"], "resources_required": 3,
                "deliverables": "Dependency Map, Impact Analysis"
            },
            {
                "wbs_id": "2.3", "phase": "Assessment", "task_name": "Data Profiling",
                "description": "Profile source and target data structures",
                "estimated_days": 12, "dependencies": ["2.1"], "resources_required": 4,
                "deliverables": "Data Profile Report, Quality Assessment"
            },
            {
                "wbs_id": "2.4", "phase": "Assessment", "task_name": "Performance Baseline",
                "description": "Establish current performance baselines",
                "estimated_days": 7, "dependencies": ["2.1"], "resources_required": 2,
                "deliverables": "Performance Baseline Report, KPI Dashboard"
            },
            
            # Phase 2: Design
            {
                "wbs_id": "3.0", "phase": "Design", "task_name": "Target Architecture Design",
                "description": "Design PySpark target architecture",
                "estimated_days": 15, "dependencies": ["2.0"], "resources_required": 4,
                "deliverables": "Architecture Blueprint, Design Document"
            },
            {
                "wbs_id": "3.1", "phase": "Design", "task_name": "Migration Pattern Definition",
                "description": "Define reusable migration patterns",
                "estimated_days": 10, "dependencies": ["3.0"], "resources_required": 3,
                "deliverables": "Pattern Library, Transformation Rules"
            },
            {
                "wbs_id": "3.2", "phase": "Design", "task_name": "Data Model Design",
                "description": "Design target data models and schemas",
                "estimated_days": 12, "dependencies": ["2.3", "3.0"], "resources_required": 4,
                "deliverables": "Data Model Diagram, Schema Definitions"
            },
            {
                "wbs_id": "3.3", "phase": "Design", "task_name": "Error Handling Framework",
                "description": "Design error handling and logging framework",
                "estimated_days": 8, "dependencies": ["3.0"], "resources_required": 2,
                "deliverables": "Error Handling Design, Logging Standards"
            },
            {
                "wbs_id": "3.4", "phase": "Design", "task_name": "Testing Strategy",
                "description": "Define comprehensive testing strategy",
                "estimated_days": 10, "dependencies": ["3.0"], "resources_required": 3,
                "deliverables": "Test Strategy, Test Plan Template"
            },
            
            # Phase 3: Development - Wave 1
            {
                "wbs_id": "4.0", "phase": "Development", "task_name": "Development Environment Setup",
                "description": "Setup development and CI/CD environments",
                "estimated_days": 10, "dependencies": ["3.0"], "resources_required": 3,
                "deliverables": "Dev Environment, CI/CD Pipeline"
            },
            {
                "wbs_id": "4.1", "phase": "Development", "task_name": "Framework Development",
                "description": "Build reusable PySpark frameworks",
                "estimated_days": 20, "dependencies": ["4.0", "3.1"], "resources_required": 5,
                "deliverables": "Core Framework, Utility Libraries"
            },
            {
                "wbs_id": "4.2", "phase": "Development", "task_name": "Wave 1 Migration - Critical Workflows",
                "description": "Migrate critical priority workflows",
                "estimated_days": 30, "dependencies": ["4.1"], "resources_required": 8,
                "deliverables": "Migrated Workflows, Unit Tests"
            },
            {
                "wbs_id": "4.3", "phase": "Development", "task_name": "Wave 2 Migration - High Priority",
                "description": "Migrate high priority workflows",
                "estimated_days": 40, "dependencies": ["4.2"], "resources_required": 8,
                "deliverables": "Migrated Workflows, Unit Tests"
            },
            {
                "wbs_id": "4.4", "phase": "Development", "task_name": "Wave 3 Migration - Medium Priority",
                "description": "Migrate medium priority workflows",
                "estimated_days": 35, "dependencies": ["4.3"], "resources_required": 6,
                "deliverables": "Migrated Workflows, Unit Tests"
            },
            {
                "wbs_id": "4.5", "phase": "Development", "task_name": "Wave 4 Migration - Low Priority",
                "description": "Migrate remaining workflows",
                "estimated_days": 25, "dependencies": ["4.4"], "resources_required": 4,
                "deliverables": "Migrated Workflows, Unit Tests"
            },
            
            # Phase 4: Testing
            {
                "wbs_id": "5.0", "phase": "Testing", "task_name": "Unit Testing",
                "description": "Execute comprehensive unit tests",
                "estimated_days": 20, "dependencies": ["4.2"], "resources_required": 4,
                "deliverables": "Unit Test Results, Coverage Report"
            },
            {
                "wbs_id": "5.1", "phase": "Testing", "task_name": "Integration Testing",
                "description": "Execute end-to-end integration tests",
                "estimated_days": 25, "dependencies": ["5.0", "4.5"], "resources_required": 6,
                "deliverables": "Integration Test Results, Defect Log"
            },
            {
                "wbs_id": "5.2", "phase": "Testing", "task_name": "Performance Testing",
                "description": "Execute performance and scalability tests",
                "estimated_days": 15, "dependencies": ["5.1"], "resources_required": 3,
                "deliverables": "Performance Test Results, Tuning Report"
            },
            {
                "wbs_id": "5.3", "phase": "Testing", "task_name": "Data Reconciliation",
                "description": "Reconcile data between Informatica and PySpark",
                "estimated_days": 20, "dependencies": ["5.1"], "resources_required": 5,
                "deliverables": "Reconciliation Report, Data Quality Metrics"
            },
            {
                "wbs_id": "5.4", "phase": "Testing", "task_name": "User Acceptance Testing",
                "description": "Conduct UAT with business stakeholders",
                "estimated_days": 15, "dependencies": ["5.2", "5.3"], "resources_required": 8,
                "deliverables": "UAT Results, Sign-off Document"
            },
            
            # Phase 5: Deployment
            {
                "wbs_id": "6.0", "phase": "Deployment", "task_name": "Production Environment Setup",
                "description": "Setup production infrastructure",
                "estimated_days": 10, "dependencies": ["5.4"], "resources_required": 3,
                "deliverables": "Production Environment, Security Config"
            },
            {
                "wbs_id": "6.1", "phase": "Deployment", "task_name": "Deployment Planning",
                "description": "Create detailed deployment plan and runbooks",
                "estimated_days": 8, "dependencies": ["6.0"], "resources_required": 4,
                "deliverables": "Deployment Plan, Runbooks, Rollback Plan"
            },
            {
                "wbs_id": "6.2", "phase": "Deployment", "task_name": "Pilot Deployment",
                "description": "Deploy pilot workflows to production",
                "estimated_days": 5, "dependencies": ["6.1"], "resources_required": 6,
                "deliverables": "Pilot Deployment Report, Lessons Learned"
            },
            {
                "wbs_id": "6.3", "phase": "Deployment", "task_name": "Phased Production Rollout",
                "description": "Rollout remaining workflows in phases",
                "estimated_days": 20, "dependencies": ["6.2"], "resources_required": 8,
                "deliverables": "Deployment Reports, Production Validation"
            },
            {
                "wbs_id": "6.4", "phase": "Deployment", "task_name": "Parallel Run",
                "description": "Run both systems in parallel for validation",
                "estimated_days": 15, "dependencies": ["6.3"], "resources_required": 6,
                "deliverables": "Parallel Run Report, Cutover Decision"
            },
            
            # Phase 6: Hypercare and Closure
            {
                "wbs_id": "7.0", "phase": "Hypercare", "task_name": "Hypercare Support",
                "description": "Provide intensive post-deployment support",
                "estimated_days": 30, "dependencies": ["6.4"], "resources_required": 10,
                "deliverables": "Support Tickets, Stabilization Report"
            },
            {
                "wbs_id": "7.1", "phase": "Hypercare", "task_name": "Performance Tuning",
                "description": "Optimize production performance",
                "estimated_days": 15, "dependencies": ["7.0"], "resources_required": 4,
                "deliverables": "Tuning Report, Performance Metrics"
            },
            {
                "wbs_id": "7.2", "phase": "Hypercare", "task_name": "Knowledge Transfer",
                "description": "Transfer knowledge to support team",
                "estimated_days": 10, "dependencies": ["7.0"], "resources_required": 6,
                "deliverables": "Training Materials, Support Documentation"
            },
            {
                "wbs_id": "8.0", "phase": "Closure", "task_name": "Project Closure",
                "description": "Complete project closure activities",
                "estimated_days": 10, "dependencies": ["7.0", "7.1", "7.2"], "resources_required": 3,
                "deliverables": "Closure Report, Lessons Learned, Final Budget"
            },
            {
                "wbs_id": "8.1", "phase": "Closure", "task_name": "Decommission Informatica",
                "description": "Decommission legacy Informatica environment",
                "estimated_days": 15, "dependencies": ["8.0"], "resources_required": 4,
                "deliverables": "Decommissioning Report, Archive Plan"
            }
        ]
        
        # Create DataFrame from WBS data
        wbs_df = self.spark.createDataFrame(wbs_data)
        
        # Add calculated columns
        wbs_df = wbs_df.withColumn("estimated_hours", col("estimated_days") * 8) \
                       .withColumn("effort_person_days", col("estimated_days") * col("resources_required")) \
                       .withColumn("project_id", lit(self.project_id)) \
                       .withColumn("created_timestamp", current_timestamp())
        
        logger.info(f"Created WBS with {wbs_df.count()} tasks")
        return wbs_df
    
    def calculate_project_timeline(self, wbs_df: DataFrame, start_date: str) -> DataFrame:
        """
        Calculate project timeline with dependencies and critical path
        
        Args:
            wbs_df: Work breakdown structure DataFrame
            start_date: Project start date (YYYY-MM-DD format)
            
        Returns:
            DataFrame with calculated dates and critical path
        """
        logger.info(f"Calculating project timeline from start date: {start_date}")
        
        # Convert dependencies string to array for processing
        timeline_df = wbs_df.withColumn(
            "dependency_array",
            when(col("dependencies").isNotNull(), col("dependencies"))
            .otherwise(array())
        )
        
        # Add start date and calculate end dates
        timeline_df = timeline_df.withColumn("planned_start_date", lit(start_date).cast(DateType())) \
                                 .withColumn("planned_end_date", 
                                           date_add(col("planned_start_date"), 
                                                   col("estimated_days").cast(IntegerType())))
        
        # Calculate critical path indicators
        timeline_df = timeline_df.withColumn(
            "is_critical_path",
            when(col("dependencies").isNotNull() & (col("estimated_days") > 10), lit(True))
            .otherwise(lit(False))
        )
        
        # Add buffer days for risk mitigation (20% buffer)
        timeline_df = timeline_df.withColumn(
            "buffer_days",
            (col("estimated_days") * 0.2).cast(IntegerType())
        ).withColumn(
            "buffered_end_date",
            date_add(col("planned_end_date"), col("buffer_days"))
        )
        
        # Calculate project milestones
        window_spec = Window.partitionBy("phase").orderBy("wbs_id")
        timeline_df = timeline_df.withColumn(
            "is_milestone",
            when(
                (row_number().over(window_spec) == 1) |
                (lead("phase").over(Window.orderBy("wbs_id")) != col("phase")),
                lit(True)
            ).otherwise(lit(False))
        )
        
        logger.info("Timeline calculation completed")
        return timeline_df
    
    def allocate_resources(self, timeline_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Allocate resources to tasks and track capacity
        
        Args:
            timeline_df: Timeline DataFrame with task details
            
        Returns:
            Tuple of (resource_allocation_df, capacity_analysis_df)
        """
        logger.info("Performing resource allocation")
        
        # Define resource pool
        resource_pool = [
            {"resource_id": "PM001", "name": "John Smith", "role": "Project Manager", 
             "hourly_rate": 150, "capacity_hours_per_day": 8, "availability": 1.0},
            {"resource_id": "ARCH001", "name": "Sarah Johnson", "role": "Solution Architect",
             "hourly_rate": 175, "capacity_hours_per_day": 8, "availability": 0.75},
            {"resource_id": "LEAD001", "name": "Mike Chen", "role": "Lead Data Engineer",
             "hourly_rate": 140, "capacity_hours_per_day": 8, "availability": 1.0},
            {"resource_id": "SENG001", "name": "Emily Brown", "role": "Senior Data Engineer",
             "hourly_rate": 120, "capacity_hours_per_day": 8, "availability": 1.0},
            {"resource_id": "SENG002", "name": "David Lee", "role": "Senior Data Engineer",
             "hourly_rate": 120, "capacity_hours_per_day": 8, "availability": 1.0},
            {"resource_id": "ENG001", "name": "Anna Martinez", "role": "Data Engineer",
             "hourly_rate": 100, "capacity_hours_per_day": 8, "availability": 1.0},
            {"resource_id": "ENG002", "name": "James Wilson", "role": "Data Engineer",
             "hourly_rate": 100, "capacity_hours_per_day": 8, "availability": 1.0},
            {"resource_id": "ENG003", "name": "Lisa Anderson", "role": "Data Engineer",
             "hourly_rate": 100, "capacity_hours_per_day": 8, "availability": 1.0},
            {"resource_id": "QLEAD001", "name": "Robert Taylor", "role": "QA Lead",
             "hourly_rate": 110, "capacity_hours_per_day": 8, "availability": 0.8},
            {"resource_id": "QA001", "name": "Jennifer White", "role": "QA Engineer",
             "hourly_rate": 90, "capacity_hours_per_day": 8, "availability": 1.0},
            {"resource_id": "QA002", "name": "Michael Garcia", "role": "QA Engineer",
             "hourly_rate": 90, "capacity_hours_per_day": 8, "availability": 1.0},
            {"resource_id": "DEVOPS001", "name": "Chris Martin", "role": "DevOps Engineer",
             "hourly_rate": 130, "capacity_hours_per_day": 8, "availability": 0.6},
            {"resource_id": "BA001", "name": "Patricia Davis", "role": "Business Analyst",
             "hourly_rate": 105, "capacity_hours_per_day": 8, "availability": 0.8},
            {"resource_id": "SME001", "name": "Daniel Rodriguez", "role": "Subject Matter Expert",
             "hourly_rate": 125, "capacity_hours_per_day": 4, "availability": 0.5}
        ]
        
        resources_df = self.spark.createDataFrame(resource_pool)
        
        # Calculate resource allocation based on task requirements
        allocation_df = timeline_df.alias("t").crossJoin(
            resources_df.alias("r")
        ).select(
            col("t.wbs_id"),
            col("t.task_name"),
            col("t.phase"),
            col("t.estimated_hours"),
            col("t.resources_required"),
            col("t.planned_start_date"),
            col("t.planned_end_date"),
            col("r.resource_id"),
            col("r.name").alias("resource_name"),
            col("r.role"),
            col("r.hourly_rate"),
            col("r.capacity_hours_per_day"),
            col("r.availability")
        )
        
        # Allocate hours per resource based on requirements
        allocation_df = allocation_df.withColumn(
            "allocated_hours",
            (col("estimated_hours") / col("resources_required"))
        ).withColumn(
            "estimated_cost",
            col("allocated_hours") * col("hourly_rate")
        )
        
        # Calculate capacity utilization
        capacity_df = allocation_df.groupBy(
            "resource_id", "resource_name", "role", "capacity_hours_per_day", "availability"
        ).agg(
            _sum("allocated_hours").alias("total_allocated_hours"),
            _sum("estimated_cost").alias("total_cost"),
            count("wbs_id").alias("task_count")
        ).withColumn(
            "available_hours",
            col("capacity_hours_per_day") * col("availability") * 220  # ~220 working days per year
        ).withColumn(
            "utilization_percentage",
            (col("total_allocated_hours") / col("available_hours") * 100).cast(DecimalType(5, 2))
        ).withColumn(
            "capacity_status",
            when(col("utilization_percentage") > 100, lit("Overallocated"))
            .when(col("utilization_percentage") > 85, lit("At Capacity"))
            .when(col("utilization_percentage") > 70, lit("Well Utilized"))
            .otherwise(lit("Underutilized"))
        )
        
        logger.info(f"Resource allocation completed for {resources_df.count()} resources")
        return allocation_df, capacity_df
    
    def define_success_metrics(self) -> DataFrame:
        """
        Define comprehensive success metrics and KPIs for migration project
        
        Returns:
            DataFrame containing all success metrics and targets
        """
        logger.info("Defining success metrics and KPIs")
        
        metrics_data = [
            # Schedule Metrics
            {
                "metric_category": "Schedule", "metric_name": "Schedule Variance (SV)",
                "description": "Difference between planned and actual completion",
                "target_value": 0.0, "baseline_value": 0.0, "unit": "days",
                "measurement_frequency": "Weekly", "threshold_warning": 5.0,
                "threshold_critical": 10.0, "calculation_formula": "Planned_Date - Actual_Date"
            },
            {
                "metric_category": "Schedule", "metric_name": "Schedule Performance Index (SPI)",
                "description": "Ratio of work performed to work scheduled",
                "target_value": 1.0, "baseline_value": 1.0, "unit": "ratio",
                "measurement_frequency": "Weekly", "threshold_warning": 0.9,
                "threshold_critical": 0.8, "calculation_formula": "EV / PV"
            },
            {
                "metric_category": "Schedule", "metric_name": "On-Time Delivery Rate",
                "description": "Percentage of tasks completed on schedule",
                "target_value": 95.0, "baseline_value": 0.0, "unit": "percentage",
                "measurement_frequency": "Weekly", "threshold_warning": 85.0,
                "threshold_critical": 75.0, "calculation_formula": "(On_Time_Tasks / Total_Tasks) * 100"
            },
            
            # Cost Metrics
            {
                "metric_category": "Cost", "metric_name": "Cost Variance (CV)",
                "description": "Difference between budgeted and actual cost",
                "target_value": 0.0, "baseline_value": 0.0, "unit": "USD",
                "measurement_frequency": "Weekly", "threshold_warning": 50000.0,
                "threshold_critical": 100000.0, "calculation_formula": "EV - AC"
            },
            {
                "metric_category": "Cost", "metric_name": "Cost Performance Index (CPI)",
                "description": "Value of work performed vs actual cost",
                "target_value": 1.0, "baseline_value": 1.0, "unit": "ratio",
                "measurement_frequency": "Weekly", "threshold_warning": 0.9,
                "threshold_critical": 0.85, "calculation_formula": "EV / AC"
            },
            {
                "metric_category": "Cost", "metric_name": "Budget Utilization",
                "description": "Percentage of budget consumed",
                "target_value": 95.0, "baseline_value": 0.0, "unit": "percentage",
                "measurement_frequency": "Weekly", "threshold_warning": 105.0,
                "threshold_critical": 110.0, "calculation_formula": "(Actual_Cost / Budget) * 100"
            },
            
            # Quality Metrics
            {
                "metric_category": "Quality", "metric_name": "Defect Density",
                "description": "Number of defects per workflow migrated",
                "target_value": 2.0, "baseline_value": 0.0, "unit": "count",
                "measurement_frequency": "Weekly", "threshold_warning": 5.0,
                "threshold_critical": 10.0, "calculation_formula": "Total_Defects / Total_Workflows"
            },
            {
                "metric_category": "Quality", "metric_name": "Data Reconciliation Accuracy",
                "description": "Percentage of records matching between systems",
                "target_value": 99.99, "baseline_value": 0.0, "unit": "percentage",
                "measurement_frequency": "Daily", "threshold_warning": 99.9,
                "threshold_critical": 99.5, "calculation_formula": "(Matching_Records / Total_Records) * 100"
            },
            {
                "metric_category": "Quality", "metric_name": "Test Coverage",
                "description": "Percentage of code covered by automated tests",
                "target_value": 85.0, "baseline_value": 0.0, "unit": "percentage",
                "measurement_frequency": "Daily", "threshold_warning": 75.0,
                "threshold_critical": 65.