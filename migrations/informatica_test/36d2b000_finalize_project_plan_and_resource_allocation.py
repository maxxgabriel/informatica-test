# PySpark Migration Project Plan - Configuration and Tracking Framework

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json

# Initialize Spark Session for Project Management and Tracking
spark = SparkSession.builder \
    .appName("InformaticaToPySparkMigrationProjectPlan") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# ============================================================================
# 1. WORK BREAKDOWN STRUCTURE (WBS)
# ============================================================================

wbs_schema = StructType([
    StructField("wbs_id", StringType(), False),
    StructField("wbs_level", IntegerType(), False),
    StructField("parent_wbs_id", StringType(), True),
    StructField("task_name", StringType(), False),
    StructField("task_description", StringType(), True),
    StructField("phase", StringType(), False),
    StructField("estimated_hours", DoubleType(), False),
    StructField("estimated_cost", DoubleType(), False),
    StructField("complexity", StringType(), False),
    StructField("priority", IntegerType(), False),
    StructField("created_date", TimestampType(), False)
])

wbs_data = [
    # Phase 1: Assessment and Planning
    ("1.0", 1, None, "Assessment and Planning", "Complete assessment and project setup", "Assessment", 800, 120000, "High", 1, datetime.now()),
    ("1.1", 2, "1.0", "Current State Analysis", "Analyze existing Informatica workflows", "Assessment", 120, 18000, "High", 1, datetime.now()),
    ("1.1.1", 3, "1.1", "Inventory Informatica Workflows", "Document all workflows and mappings", "Assessment", 40, 6000, "Medium", 1, datetime.now()),
    ("1.1.2", 3, "1.1", "Analyze Dependencies", "Map workflow dependencies and data lineage", "Assessment", 40, 6000, "High", 1, datetime.now()),
    ("1.1.3", 3, "1.1", "Performance Baseline", "Establish current performance metrics", "Assessment", 40, 6000, "Medium", 1, datetime.now()),
    ("1.2", 2, "1.0", "Target Architecture Design", "Design PySpark target architecture", "Assessment", 160, 24000, "High", 1, datetime.now()),
    ("1.2.1", 3, "1.2", "Infrastructure Design", "Design Spark cluster and infrastructure", "Assessment", 60, 9000, "High", 1, datetime.now()),
    ("1.2.2", 3, "1.2", "Data Architecture", "Design data lake and storage architecture", "Assessment", 60, 9000, "High", 1, datetime.now()),
    ("1.2.3", 3, "1.2", "Security Architecture", "Design security and governance framework", "Assessment", 40, 6000, "High", 1, datetime.now()),
    ("1.3", 2, "1.0", "Migration Strategy", "Define migration approach and waves", "Assessment", 120, 18000, "High", 1, datetime.now()),
    ("1.3.1", 3, "1.3", "Wave Planning", "Organize workflows into migration waves", "Assessment", 40, 6000, "Medium", 1, datetime.now()),
    ("1.3.2", 3, "1.3", "Risk Assessment", "Identify and assess migration risks", "Assessment", 40, 6000, "High", 1, datetime.now()),
    ("1.3.3", 3, "1.3", "Testing Strategy", "Define testing approach and criteria", "Assessment", 40, 6000, "High", 1, datetime.now()),
    ("1.4", 2, "1.0", "POC Development", "Build and validate proof of concept", "Assessment", 200, 30000, "High", 1, datetime.now()),
    ("1.4.1", 3, "1.4", "Select POC Workflows", "Choose representative workflows for POC", "Assessment", 20, 3000, "Low", 1, datetime.now()),
    ("1.4.2", 3, "1.4", "Develop PySpark POC", "Convert selected workflows to PySpark", "Assessment", 120, 18000, "High", 1, datetime.now()),
    ("1.4.3", 3, "1.4", "POC Validation", "Test and validate POC results", "Assessment", 60, 9000, "Medium", 1, datetime.now()),
    ("1.5", 2, "1.0", "Project Setup", "Establish project governance and tools", "Assessment", 200, 30000, "Medium", 1, datetime.now()),
    ("1.5.1", 3, "1.5", "Team Formation", "Assemble and onboard project team", "Assessment", 80, 12000, "Medium", 1, datetime.now()),
    ("1.5.2", 3, "1.5", "Tool Setup", "Configure project management and dev tools", "Assessment", 60, 9000, "Low", 1, datetime.now()),
    ("1.5.3", 3, "1.5", "Governance Framework", "Establish governance and RACI matrix", "Assessment", 60, 9000, "Medium", 1, datetime.now()),
    
    # Phase 2: Infrastructure Setup
    ("2.0", 1, None, "Infrastructure Setup", "Build and configure target infrastructure", "Infrastructure", 600, 90000, "High", 2, datetime.now()),
    ("2.1", 2, "2.0", "Spark Cluster Setup", "Deploy and configure Spark clusters", "Infrastructure", 200, 30000, "High", 2, datetime.now()),
    ("2.1.1", 3, "2.1", "Cluster Deployment", "Deploy Spark clusters in target environment", "Infrastructure", 80, 12000, "High", 2, datetime.now()),
    ("2.1.2", 3, "2.1", "Configuration Tuning", "Optimize cluster configurations", "Infrastructure", 60, 9000, "Medium", 2, datetime.now()),
    ("2.1.3", 3, "2.1", "Monitoring Setup", "Configure monitoring and alerting", "Infrastructure", 60, 9000, "Medium", 2, datetime.now()),
    ("2.2", 2, "2.0", "Data Storage Setup", "Configure data lake and storage layers", "Infrastructure", 160, 24000, "High", 2, datetime.now()),
    ("2.2.1", 3, "2.2", "Data Lake Configuration", "Setup S3/HDFS/ADLS storage", "Infrastructure", 80, 12000, "High", 2, datetime.now()),
    ("2.2.2", 3, "2.2", "Hive Metastore Setup", "Configure metadata management", "Infrastructure", 80, 12000, "Medium", 2, datetime.now()),
    ("2.3", 2, "2.0", "Security Implementation", "Implement security controls", "Infrastructure", 120, 18000, "High", 2, datetime.now()),
    ("2.3.1", 3, "2.3", "Authentication Setup", "Configure Kerberos/OAuth", "Infrastructure", 60, 9000, "High", 2, datetime.now()),
    ("2.3.2", 3, "2.3", "Authorization Framework", "Setup Ranger/ACLs", "Infrastructure", 60, 9000, "High", 2, datetime.now()),
    ("2.4", 2, "2.0", "CI/CD Pipeline", "Build deployment automation", "Infrastructure", 120, 18000, "Medium", 2, datetime.now()),
    ("2.4.1", 3, "2.4", "Pipeline Development", "Create CI/CD pipelines", "Infrastructure", 80, 12000, "Medium", 2, datetime.now()),
    ("2.4.2", 3, "2.4", "Automated Testing", "Implement automated test framework", "Infrastructure", 40, 6000, "Medium", 2, datetime.now()),
    
    # Phase 3: Framework Development
    ("3.0", 1, None, "Framework Development", "Build reusable migration framework", "Development", 1200, 180000, "High", 3, datetime.now()),
    ("3.1", 2, "3.0", "Core Framework", "Develop core PySpark framework", "Development", 400, 60000, "High", 3, datetime.now()),
    ("3.1.1", 3, "3.1", "Base Classes", "Create base classes and utilities", "Development", 120, 18000, "High", 3, datetime.now()),
    ("3.1.2", 3, "3.1", "Configuration Management", "Build configuration framework", "Development", 80, 12000, "Medium", 3, datetime.now()),
    ("3.1.3", 3, "3.1", "Error Handling", "Implement error handling and logging", "Development", 80, 12000, "Medium", 3, datetime.now()),
    ("3.1.4", 3, "3.1", "Data Quality Framework", "Build data quality validation", "Development", 120, 18000, "High", 3, datetime.now()),
    ("3.2", 2, "3.0", "Transformation Library", "Build transformation components", "Development", 400, 60000, "High", 3, datetime.now()),
    ("3.2.1", 3, "3.2", "Source Connectors", "Develop source system connectors", "Development", 120, 18000, "High", 3, datetime.now()),
    ("3.2.2", 3, "3.2", "Target Connectors", "Develop target system connectors", "Development", 80, 12000, "Medium", 3, datetime.now()),
    ("3.2.3", 3, "3.2", "Transformation Functions", "Build common transformation functions", "Development", 120, 18000, "High", 3, datetime.now()),
    ("3.2.4", 3, "3.2", "SCD Implementation", "Implement SCD Type 1/2 logic", "Development", 80, 12000, "High", 3, datetime.now()),
    ("3.3", 2, "3.0", "Orchestration Framework", "Build workflow orchestration", "Development", 200, 30000, "Medium", 3, datetime.now()),
    ("3.3.1", 3, "3.3", "Airflow DAG Framework", "Create Airflow DAG templates", "Development", 100, 15000, "Medium", 3, datetime.now()),
    ("3.3.2", 3, "3.3", "Dependency Management", "Implement dependency resolution", "Development", 60, 9000, "Medium", 3, datetime.now()),
    ("3.3.3", 3, "3.3", "Scheduling Framework", "Build scheduling components", "Development", 40, 6000, "Low", 3, datetime.now()),
    ("3.4", 2, "3.0", "Testing Framework", "Build automated testing suite", "Development", 200, 30000, "High", 3, datetime.now()),
    ("3.4.1", 3, "3.4", "Unit Test Framework", "Create unit test templates", "Development", 80, 12000, "Medium", 3, datetime.now()),
    ("3.4.2", 3, "3.4", "Integration Tests", "Build integration test suite", "Development", 80, 12000, "Medium", 3, datetime.now()),
    ("3.4.3", 3, "3.4", "Reconciliation Tools", "Develop data reconciliation utilities", "Development", 40, 6000, "High", 3, datetime.now()),
    
    # Phase 4: Wave 1 Migration (Critical Workflows)
    ("4.0", 1, None, "Wave 1 Migration", "Migrate critical priority workflows", "Migration", 2000, 300000, "High", 4, datetime.now()),
    ("4.1", 2, "4.0", "Analysis Phase", "Analyze Wave 1 workflows", "Migration", 400, 60000, "High", 4, datetime.now()),
    ("4.1.1", 3, "4.1", "Workflow Analysis", "Detail analysis of 20 critical workflows", "Migration", 200, 30000, "High", 4, datetime.now()),
    ("4.1.2", 3, "4.1", "Data Profiling", "Profile source and target data", "Migration", 120, 18000, "Medium", 4, datetime.now()),
    ("4.1.3", 3, "4.1", "Design Review", "Review and approve design", "Migration", 80, 12000, "Medium", 4, datetime.now()),
    ("4.2", 2, "4.0", "Development Phase", "Convert workflows to PySpark", "Migration", 800, 120000, "High", 4, datetime.now()),
    ("4.2.1", 3, "4.2", "Code Development", "Develop PySpark jobs", "Migration", 600, 90000, "High", 4, datetime.now()),
    ("4.2.2", 3, "4.2", "Code Review", "Peer review and quality checks", "Migration", 120, 18000, "Medium", 4, datetime.now()),
    ("4.2.3", 3, "4.2", "Unit Testing", "Execute unit tests", "Migration", 80, 12000, "Medium", 4, datetime.now()),
    ("4.3", 2, "4.0", "Testing Phase", "Comprehensive testing", "Migration", 600, 90000, "High", 4, datetime.now()),
    ("4.3.1", 3, "4.3", "System Testing", "End-to-end system testing", "Migration", 240, 36000, "High", 4, datetime.now()),
    ("4.3.2", 3, "4.3", "Performance Testing", "Performance and load testing", "Migration", 160, 24000, "High", 4, datetime.now()),
    ("4.3.3", 3, "4.3", "UAT", "User acceptance testing", "Migration", 120, 18000, "High", 4, datetime.now()),
    ("4.3.4", 3, "4.3", "Data Reconciliation", "Validate data accuracy", "Migration", 80, 12000, "High", 4, datetime.now()),
    ("4.4", 2, "4.0", "Deployment Phase", "Production deployment", "Migration", 200, 30000, "High", 4, datetime.now()),
    ("4.4.1", 3, "4.4", "Deployment Planning", "Create deployment runbook", "Migration", 40, 6000, "Medium", 4, datetime.now()),
    ("4.4.2", 3, "4.4", "Production Deployment", "Deploy to production", "Migration", 80, 12000, "High", 4, datetime.now()),
    ("4.4.3", 3, "4.4", "Post-Deployment Validation", "Validate production runs", "Migration", 80, 12000, "High", 4, datetime.now()),
    
    # Phase 5: Wave 2 Migration (High Priority)
    ("5.0", 1, None, "Wave 2 Migration", "Migrate high priority workflows", "Migration", 3000, 450000, "High", 5, datetime.now()),
    ("5.1", 2, "5.0", "Analysis Phase", "Analyze Wave 2 workflows", "Migration", 600, 90000, "High", 5, datetime.now()),
    ("5.2", 2, "5.0", "Development Phase", "Convert workflows to PySpark", "Migration", 1200, 180000, "High", 5, datetime.now()),
    ("5.3", 2, "5.0", "Testing Phase", "Comprehensive testing", "Migration", 900, 135000, "High", 5, datetime.now()),
    ("5.4", 2, "5.0", "Deployment Phase", "Production deployment", "Migration", 300, 45000, "High", 5, datetime.now()),
    
    # Phase 6: Wave 3 Migration (Medium Priority)
    ("6.0", 1, None, "Wave 3 Migration", "Migrate medium priority workflows", "Migration", 2400, 360000, "Medium", 6, datetime.now()),
    ("6.1", 2, "6.0", "Analysis Phase", "Analyze Wave 3 workflows", "Migration", 480, 72000, "Medium", 6, datetime.now()),
    ("6.2", 2, "6.0", "Development Phase", "Convert workflows to PySpark", "Migration", 960, 144000, "Medium", 6, datetime.now()),
    ("6.3", 2, "6.0", "Testing Phase", "Comprehensive testing", "Migration", 720, 108000, "Medium", 6, datetime.now()),
    ("6.4", 2, "6.0", "Deployment Phase", "Production deployment", "Migration", 240, 36000, "Medium", 6, datetime.now()),
    
    # Phase 7: Wave 4 Migration (Low Priority)
    ("7.0", 1, None, "Wave 4 Migration", "Migrate remaining workflows", "Migration", 1600, 240000, "Low", 7, datetime.now()),
    ("7.1", 2, "7.0", "Analysis Phase", "Analyze Wave 4 workflows", "Migration", 320, 48000, "Low", 7, datetime.now()),
    ("7.2", 2, "7.0", "Development Phase", "Convert workflows to PySpark", "Migration", 640, 96000, "Low", 7, datetime.now()),
    ("7.3", 2, "7.0", "Testing Phase", "Comprehensive testing", "Migration", 480, 72000, "Low", 7, datetime.now()),
    ("7.4", 2, "7.0", "Deployment Phase", "Production deployment", "Migration", 160, 24000, "Low", 7, datetime.now()),
    
    # Phase 8: Decommissioning
    ("8.0", 1, None, "Decommissioning", "Decommission Informatica platform", "Closure", 400, 60000, "Medium", 8, datetime.now()),
    ("8.1", 2, "8.0", "Parallel Run", "Execute parallel runs and validation", "Closure", 160, 24000, "High", 8, datetime.now()),
    ("8.2", 2, "8.0", "Knowledge Transfer", "Complete documentation and training", "Closure", 120, 18000, "Medium", 8, datetime.now()),
    ("8.3", 2, "8.0", "Informatica Shutdown", "Deactivate Informatica environment", "Closure", 80, 12000, "Medium", 8, datetime.now()),
    ("8.4", 2, "8.0", "Project Closure", "Complete project closure activities", "Closure", 40, 6000, "Low", 8, datetime.now())
]

df_wbs = spark.createDataFrame(wbs_data, wbs_schema)

# ============================================================================
# 2. PROJECT TIMELINE AND MILESTONES
# ============================================================================

timeline_schema = StructType([
    StructField("milestone_id", StringType(), False),
    StructField("milestone_name", StringType(), False),
    StructField("wbs_id", StringType(), False),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), False),
    StructField("duration_days", IntegerType(), False),
    StructField("predecessor", StringType(), True),
    StructField("dependency_type", StringType(), True),
    StructField("is_critical_path", BooleanType(), False),
    StructField("buffer_days", IntegerType(), False),
    StructField("status", StringType(), False)
])

base_date = datetime(2024, 1, 15)

timeline_data = [
    # Phase 1 milestones
    ("M1.1", "Assessment Complete", "1.1", base_date, base_date + timedelta(days=15), 15, None, None, True, 0, "Planned"),
    ("M1.2", "Architecture Design Complete", "1.2", base_date + timedelta(days=15), base_date + timedelta(days=35), 20, "M1.1", "FS", True, 0, "Planned"),
    ("M1.3", "Migration Strategy Approved", "1.3", base_date + timedelta(days=35), base_date + timedelta(days=50), 15, "M1.2", "FS", True, 0, "Planned"),
    ("M1.4", "POC Validated", "1.4", base_date + timedelta(days=35), base_date + timedelta(days=60), 25, "M1.2", "FS", True, 0, "Planned"),
    ("M1.5", "Project Kickoff Complete", "1.5", base_date, base_date + timedelta(days=25), 25, None, None, False, 3, "Planned"),
    
    # Phase 2 milestones
    ("M2.1", "Infrastructure Deployed", "2.1", base_date + timedelta(days=60), base_date + timedelta(days=85), 25, "M1.4", "FS", True, 0, "Planned"),
    ("M2.2", "Data Lake Configured", "2.2", base_date + timedelta(days=60), base_date + timedelta(days=80), 20, "M1.4", "FS", False, 5, "Planned"),
    ("M2.3", "Security Implemented", "2.3", base_date + timedelta(days=80), base_date + timedelta(days=95), 15, "M2.2", "FS", True, 0, "Planned"),
    ("M2.4", "CI/CD Pipeline Ready", "2.4", base_date + timedelta(days=85), base_date + timedelta(days=100), 15, "M2.1", "FS", False, 5, "Planned"),
    
    # Phase 3 milestones
    ("M3.1", "Core Framework Complete", "3.1", base_date + timedelta(days=95), base_date + timedelta(days=145), 50, "M2.3", "FS", True, 0, "Planned"),
    ("M3.2", "Transformation Library Complete", "3.2", base_date + timedelta(days=100), base_date + timedelta(days=150), 50, "M2.4", "FS", False, 5, "Planned"),
    ("M3.3", "Orchestration Framework Ready", "3.3", base_date + timedelta(days=145), base_date + timedelta(days=170), 25, "M3.1", "FS", False, 5, "Planned"),
    ("M3.4", "Testing Framework Complete", "3.4", base_date + timedelta(days=145), base_date + timedelta(days=170), 25, "M3.1", "FS", True, 0, "Planned"),
    
    # Phase 4 milestones - Wave 1
    ("M4.1", "Wave 1 Analysis Complete", "4.1", base_date + timedelta(days=170), base_date + timedelta(days=220), 50, "M3.4", "FS", True, 0, "Planned"),
    ("M4.2", "Wave 1 Development Complete", "4.2", base_date + timedelta(days=220), base_date + timedelta(days=320), 100, "M4.1", "FS", True, 0, "Planned"),
    ("M4.3", "Wave 1 Testing Complete", "4.3", base_date + timedelta(days=320), base_date + timedelta(days=395), 75, "M4.2", "FS", True, 0, "Planned"),
    ("M4.4", "Wave 1 Production Deployment", "4.4", base_date + timedelta(days=395), base_date + timedelta(days=420), 25, "M4.3", "FS", True, 0, "Planned"),
    
    # Phase 5 milestones - Wave 2
    ("M5.1", "Wave 2 Analysis Complete", "5.1", base_date + timedelta(days=420), base_date + timedelta(days=495), 75, "M4.4", "FS", True, 0, "Planned"),
    ("M5.2", "Wave 2 Development Complete", "5.2", base_date + timedelta(days=495), base_date + timedelta(days=645), 150, "M5.1", "FS", True, 0, "Planned"),
    ("M5.3", "Wave 2 Testing Complete", "5.3", base_date + timedelta(days=645), base_date + timedelta(days=758), 113, "M5.2", "FS", True, 0, "Planned"),
    ("M5.4", "Wave 2 Production Deployment", "5.4", base_date + timedelta(days=758), base_date + timedelta(days=795), 37, "M5.3", "FS", True, 0, "Planned"),
    
    # Phase 6 milestones - Wave 3
    ("M6.1", "Wave 3 Analysis Complete", "6.1", base_date + timedelta(days=795), base_date + timedelta(days=855), 60, "M5.4", "FS", False, 10, "Planned"),
    ("M6.2", "Wave 3 Development Complete", "6.2", base_date + timedelta(days=855), base_date + timedelta(days=975), 120, "M6.1", "FS", False, 10, "Planned"),
    ("M6.3", "Wave 3 Testing Complete", "6.3", base_date + timedelta(days=975), base_date + timedelta(days=1065), 90, "M6.2", "FS", False, 10, "Planned"),
    ("M6.4", "Wave 3 Production Deployment", "6.4", base_date + timedelta(days=1065), base_date + timedelta(days=1095), 30, "M6.3", "FS", False, 5, "Planned"),
    
    # Phase 7 milestones - Wave 4
    ("M7.1", "Wave 4 Analysis Complete", "7.1", base_date + timedelta(days=1095), base_date + timedelta(days=1135), 40, "M6.4", "FS", False, 10, "Planned"),
    ("M7.2", "Wave 4 Development Complete", "7.2", base_date + timedelta(days=1135), base_date + timedelta(days=1215), 80, "M7.1", "FS", False, 10, "Planned"),
    ("M7.3", "Wave 4 Testing Complete", "7.3", base_date + timedelta(days=1215), base_date + timedelta(days=1275), 60, "M7.2", "FS", False, 10, "Planned"),
    ("M7.4", "Wave 4 Production Deployment", "7.4", base_date + timedelta(days=1275), base_date + timedelta(days=1295), 20, "M7.3", "FS", False, 5, "Planned"),
    
    # Phase 8 milestones
    ("M8.1", "Parallel Run Validated", "8.1", base_date + timedelta(days=1295), base_date + timedelta(days=1315), 20, "M7.4", "FS", True, 0, "Planned"),
    ("M8.2", "Knowledge Transfer Complete", "8.2", base_date + timedelta(days=1295), base_date + timedelta(days=1310), 15, "M7.4", "FS", False, 5, "Planned"),
    ("M8.3", "Informatica Decommissioned", "8.3", base_date + timedelta(days=1315), base_date + timedelta(days=1325), 10, "M8.1", "FS", True, 0, "Planned"),
    ("M8.4", "Project Closure Complete", "8.4", base_date + timedelta(days=1325), base_date + timedelta(days=1330), 5, "M8.3", "FS", True, 0, "Planned")
]

df_timeline = spark.createDataFrame(timeline_data, timeline_schema)

# ============================================================================
# 3. RESOURCE ALLOCATION
# ============================================================================

resource_schema = StructType([
    StructField("resource_id", StringType(), False),
    StructField("resource_name", StringType(), False),
    StructField("role", StringType(), False),
    StructField("skill_level", StringType(), False),
    StructField("hourly_rate", DoubleType(), False),
    StructField("availability_percent", IntegerType(), False),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), False),
    StructField("location", StringType(), False),
    StructField("allocation_status", StringType(), False)
])

resource_data = [
    # Project Management
    ("R001", "Project Manager", "Project Manager", "Senior", 150, 100, base_date, base_date + timedelta(days=1330), "Onsite", "Confirmed"),
    ("R002", "Program Manager", "Program Manager", "Senior", 175, 50, base_date, base_date + timedelta(days=1330), "Onsite", "Confirmed"),
    ("R003", "Scrum Master 1", "Scrum Master", "Mid", 125, 100, base_date + timedelta(days=60), base_date + timedelta(days=1330), "Onsite", "Confirmed"),
    ("R004", "Scrum Master 2", "Scrum Master", "Mid", 125, 100, base_date + timedelta(days=420), base_date + timedelta(days=1330), "Onsite", "Pending"),
    
    # Architecture
    ("R005", "Lead Architect", "Solution Architect", "Senior", 200, 100, base_date, base_date + timedelta(days=1330), "Onsite", "Confirmed"),
    ("R006", "Data Architect", "Data Architect", "Senior", 180, 100, base_date, base_date + timedelta(days=800), "Onsite", "Confirmed"),
    ("R007", "Infrastructure Architect", "Infrastructure Architect", "Senior", 180, 80, base_date + timedelta(days=60), base_date + timedelta(days=200), "Remote", "Confirmed"),
    
    # Development Team - Wave 1
    ("R008", "PySpark Lead 1", "Lead Developer", "Senior", 160, 100, base_date + timedelta(days=95), base_date + timedelta(days=420), "Onsite", "Confirmed"),
    ("R009", "PySpark Developer 1", "PySpark Developer", "Senior", 140, 100, base_date + timedelta(days=95), base_date + timedelta(days=420), "Remote", "Confirmed"),
    ("R010", "PySpark Developer 2", "PySpark Developer", "Mid", 120, 100, base_date + timedelta(days=95), base_date + timedelta(days=420), "Remote", "Confirmed"),
    ("R011