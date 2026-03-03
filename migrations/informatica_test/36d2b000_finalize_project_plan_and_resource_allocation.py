# PySpark Migration Project - Resource Allocation and Monitoring Framework

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json

# Initialize Spark Session for Project Management Analytics
spark = SparkSession.builder \
    .appName("InformaticaToPySparkMigrationProjectPlan") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Project Configuration and Constants
PROJECT_START_DATE = "2024-01-15"
PROJECT_END_DATE = "2024-12-31"
BUDGET_TOTAL = 2500000
TEAM_SIZE = 25

# Work Breakdown Structure Schema
wbs_schema = StructType([
    StructField("wbs_id", StringType(), False),
    StructField("wbs_level", IntegerType(), False),
    StructField("parent_wbs_id", StringType(), True),
    StructField("task_name", StringType(), False),
    StructField("phase", StringType(), False),
    StructField("estimated_hours", DoubleType(), False),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), False),
    StructField("status", StringType(), False),
    StructField("assigned_to", StringType(), True),
    StructField("dependencies", StringType(), True),
    StructField("critical_path", BooleanType(), False),
    StructField("completion_percentage", DoubleType(), False)
])

# Work Breakdown Structure Data
wbs_data = [
    # Phase 1: Assessment and Planning
    ("1.0", 1, None, "Assessment and Planning", "Phase 1", 800.0, "2024-01-15", "2024-02-29", "In Progress", "Project Manager", None, True, 75.0),
    ("1.1", 2, "1.0", "Current State Analysis", "Phase 1", 160.0, "2024-01-15", "2024-01-31", "Completed", "Data Architect", None, True, 100.0),
    ("1.2", 2, "1.0", "Informatica Inventory Assessment", "Phase 1", 120.0, "2024-01-15", "2024-01-31", "Completed", "Senior Developer", "1.1", True, 100.0),
    ("1.3", 2, "1.0", "Complexity Analysis and Scoring", "Phase 1", 80.0, "2024-02-01", "2024-02-15", "Completed", "Lead Architect", "1.2", True, 100.0),
    ("1.4", 2, "1.0", "Migration Strategy Definition", "Phase 1", 120.0, "2024-02-01", "2024-02-15", "In Progress", "Data Architect", "1.2", True, 80.0),
    ("1.5", 2, "1.0", "Tool Selection and POC", "Phase 1", 160.0, "2024-02-16", "2024-02-29", "Not Started", "Technical Lead", "1.4", True, 0.0),
    ("1.6", 2, "1.0", "Risk Assessment and Mitigation Planning", "Phase 1", 80.0, "2024-02-16", "2024-02-29", "Not Started", "Project Manager", "1.4", False, 0.0),
    ("1.7", 2, "1.0", "Finalize Project Plan", "Phase 1", 80.0, "2024-02-20", "2024-02-29", "Not Started", "Project Manager", "1.5,1.6", True, 0.0),
    
    # Phase 2: Infrastructure Setup
    ("2.0", 1, None, "Infrastructure and Environment Setup", "Phase 2", 640.0, "2024-03-01", "2024-03-31", "Not Started", "DevOps Lead", "1.0", True, 0.0),
    ("2.1", 2, "2.0", "Databricks/Spark Cluster Setup", "Phase 2", 120.0, "2024-03-01", "2024-03-10", "Not Started", "DevOps Engineer", "1.7", True, 0.0),
    ("2.2", 2, "2.0", "CI/CD Pipeline Configuration", "Phase 2", 160.0, "2024-03-11", "2024-03-20", "Not Started", "DevOps Engineer", "2.1", True, 0.0),
    ("2.3", 2, "2.0", "Logging and Monitoring Setup", "Phase 2", 120.0, "2024-03-11", "2024-03-20", "Not Started", "DevOps Engineer", "2.1", False, 0.0),
    ("2.4", 2, "2.0", "Security and Access Control", "Phase 2", 80.0, "2024-03-21", "2024-03-27", "Not Started", "Security Engineer", "2.2", False, 0.0),
    ("2.5", 2, "2.0", "Data Lake Architecture Setup", "Phase 2", 160.0, "2024-03-21", "2024-03-31", "Not Started", "Data Architect", "2.2", True, 0.0),
    
    # Phase 3: Framework Development
    ("3.0", 1, None, "Migration Framework Development", "Phase 3", 960.0, "2024-04-01", "2024-05-15", "Not Started", "Technical Lead", "2.0", True, 0.0),
    ("3.1", 2, "3.0", "Reusable Component Library", "Phase 3", 240.0, "2024-04-01", "2024-04-20", "Not Started", "Senior Developer", "2.5", True, 0.0),
    ("3.2", 2, "3.0", "Data Quality Framework", "Phase 3", 200.0, "2024-04-01", "2024-04-20", "Not Started", "QA Lead", "2.5", True, 0.0),
    ("3.3", 2, "3.0", "Metadata Management Framework", "Phase 3", 160.0, "2024-04-21", "2024-05-05", "Not Started", "Data Engineer", "3.1", False, 0.0),
    ("3.4", 2, "3.0", "Error Handling and Recovery", "Phase 3", 120.0, "2024-04-21", "2024-05-05", "Not Started", "Senior Developer", "3.1", True, 0.0),
    ("3.5", 2, "3.0", "Performance Optimization Utilities", "Phase 3", 160.0, "2024-05-06", "2024-05-15", "Not Started", "Performance Engineer", "3.4", True, 0.0),
    ("3.6", 2, "3.0", "Testing Framework and Automation", "Phase 3", 80.0, "2024-05-06", "2024-05-15", "Not Started", "QA Engineer", "3.2", False, 0.0),
    
    # Phase 4: Wave 1 - Simple Mappings
    ("4.0", 1, None, "Wave 1 - Simple Mapping Migration", "Phase 4", 1280.0, "2024-05-16", "2024-07-15", "Not Started", "Development Team", "3.0", True, 0.0),
    ("4.1", 2, "4.0", "Code Conversion - Simple Mappings", "Phase 4", 480.0, "2024-05-16", "2024-06-15", "Not Started", "Development Team", "3.5", True, 0.0),
    ("4.2", 2, "4.0", "Unit Testing - Wave 1", "Phase 4", 320.0, "2024-06-01", "2024-06-30", "Not Started", "QA Team", "4.1", True, 0.0),
    ("4.3", 2, "4.0", "Integration Testing - Wave 1", "Phase 4", 240.0, "2024-06-16", "2024-07-05", "Not Started", "QA Team", "4.2", True, 0.0),
    ("4.4", 2, "4.0", "Performance Testing - Wave 1", "Phase 4", 160.0, "2024-06-25", "2024-07-10", "Not Started", "Performance Team", "4.3", False, 0.0),
    ("4.5", 2, "4.0", "Production Deployment - Wave 1", "Phase 4", 80.0, "2024-07-11", "2024-07-15", "Not Started", "DevOps Team", "4.3,4.4", True, 0.0),
    
    # Phase 5: Wave 2 - Medium Complexity
    ("5.0", 1, None, "Wave 2 - Medium Complexity Migration", "Phase 5", 1920.0, "2024-07-16", "2024-09-30", "Not Started", "Development Team", "4.0", True, 0.0),
    ("5.1", 2, "5.0", "Code Conversion - Medium Mappings", "Phase 5", 800.0, "2024-07-16", "2024-08-31", "Not Started", "Development Team", "4.5", True, 0.0),
    ("5.2", 2, "5.0", "Unit Testing - Wave 2", "Phase 5", 480.0, "2024-08-01", "2024-09-10", "Not Started", "QA Team", "5.1", True, 0.0),
    ("5.3", 2, "5.0", "Integration Testing - Wave 2", "Phase 5", 320.0, "2024-08-20", "2024-09-20", "Not Started", "QA Team", "5.2", True, 0.0),
    ("5.4", 2, "5.0", "Performance Testing - Wave 2", "Phase 5", 240.0, "2024-09-01", "2024-09-25", "Not Started", "Performance Team", "5.3", False, 0.0),
    ("5.5", 2, "5.0", "Production Deployment - Wave 2", "Phase 5", 80.0, "2024-09-26", "2024-09-30", "Not Started", "DevOps Team", "5.3,5.4", True, 0.0),
    
    # Phase 6: Wave 3 - Complex Mappings
    ("6.0", 1, None, "Wave 3 - Complex Mapping Migration", "Phase 6", 2560.0, "2024-10-01", "2024-12-15", "Not Started", "Development Team", "5.0", True, 0.0),
    ("6.1", 2, "6.0", "Code Conversion - Complex Mappings", "Phase 6", 1120.0, "2024-10-01", "2024-11-15", "Not Started", "Senior Development Team", "5.5", True, 0.0),
    ("6.2", 2, "6.0", "Unit Testing - Wave 3", "Phase 6", 640.0, "2024-10-20", "2024-11-30", "Not Started", "QA Team", "6.1", True, 0.0),
    ("6.3", 2, "6.0", "Integration Testing - Wave 3", "Phase 6", 400.0, "2024-11-10", "2024-12-05", "Not Started", "QA Team", "6.2", True, 0.0),
    ("6.4", 2, "6.0", "Performance Testing - Wave 3", "Phase 6", 320.0, "2024-11-20", "2024-12-10", "Not Started", "Performance Team", "6.3", False, 0.0),
    ("6.5", 2, "6.0", "Production Deployment - Wave 3", "Phase 6", 80.0, "2024-12-11", "2024-12-15", "Not Started", "DevOps Team", "6.3,6.4", True, 0.0),
    
    # Phase 7: Closure
    ("7.0", 1, None, "Project Closure and Optimization", "Phase 7", 480.0, "2024-12-16", "2024-12-31", "Not Started", "Project Team", "6.0", True, 0.0),
    ("7.1", 2, "7.0", "Hypercare and Support", "Phase 7", 240.0, "2024-12-16", "2024-12-31", "Not Started", "Support Team", "6.5", False, 0.0),
    ("7.2", 2, "7.0", "Documentation Finalization", "Phase 7", 80.0, "2024-12-16", "2024-12-25", "Not Started", "Technical Writers", "6.5", False, 0.0),
    ("7.3", 2, "7.0", "Knowledge Transfer", "Phase 7", 80.0, "2024-12-16", "2024-12-25", "Not Started", "Project Team", "7.2", False, 0.0),
    ("7.4", 2, "7.0", "Lessons Learned Workshop", "Phase 7", 40.0, "2024-12-26", "2024-12-27", "Not Started", "Project Manager", "7.3", False, 0.0),
    ("7.5", 2, "7.0", "Project Closure Report", "Phase 7", 40.0, "2024-12-28", "2024-12-31", "Not Started", "Project Manager", "7.4", True, 0.0)
]

# Create WBS DataFrame
df_wbs = spark.createDataFrame(wbs_data, wbs_schema)

# Resource Allocation Schema
resource_schema = StructType([
    StructField("resource_id", StringType(), False),
    StructField("resource_name", StringType(), False),
    StructField("role", StringType(), False),
    StructField("skill_level", StringType(), False),
    StructField("hourly_rate", DoubleType(), False),
    StructField("availability_percentage", DoubleType(), False),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), False),
    StructField("location", StringType(), False),
    StructField("team", StringType(), False)
])

# Resource Allocation Data
resource_data = [
    ("R001", "John Smith", "Project Manager", "Senior", 150.0, 100.0, "2024-01-15", "2024-12-31", "US", "Management"),
    ("R002", "Sarah Johnson", "Data Architect", "Expert", 175.0, 100.0, "2024-01-15", "2024-12-31", "US", "Architecture"),
    ("R003", "Michael Chen", "Technical Lead", "Senior", 165.0, 100.0, "2024-01-15", "2024-12-31", "US", "Development"),
    ("R004", "Emily Davis", "Lead Architect", "Expert", 175.0, 75.0, "2024-01-15", "2024-05-31", "US", "Architecture"),
    ("R005", "David Wilson", "Senior Developer", "Senior", 140.0, 100.0, "2024-01-15", "2024-12-31", "India", "Development"),
    ("R006", "Lisa Anderson", "Senior Developer", "Senior", 140.0, 100.0, "2024-04-01", "2024-12-31", "India", "Development"),
    ("R007", "Robert Taylor", "DevOps Lead", "Senior", 150.0, 100.0, "2024-03-01", "2024-12-31", "US", "Infrastructure"),
    ("R008", "Jennifer Martinez", "DevOps Engineer", "Mid", 120.0, 100.0, "2024-03-01", "2024-12-31", "India", "Infrastructure"),
    ("R009", "James Brown", "QA Lead", "Senior", 135.0, 100.0, "2024-04-01", "2024-12-31", "India", "Quality"),
    ("R010", "Maria Garcia", "QA Engineer", "Mid", 110.0, 100.0, "2024-05-01", "2024-12-31", "India", "Quality"),
    ("R011", "William Rodriguez", "Data Engineer", "Mid", 125.0, 100.0, "2024-04-01", "2024-12-31", "India", "Development"),
    ("R012", "Patricia Hernandez", "Data Engineer", "Mid", 125.0, 100.0, "2024-05-01", "2024-12-31", "India", "Development"),
    ("R013", "Thomas Lee", "Data Engineer", "Junior", 95.0, 100.0, "2024-05-15", "2024-12-31", "India", "Development"),
    ("R014", "Linda White", "Data Engineer", "Junior", 95.0, 100.0, "2024-05-15", "2024-12-31", "India", "Development"),
    ("R015", "Christopher Harris", "Data Engineer", "Junior", 95.0, 100.0, "2024-07-01", "2024-12-31", "India", "Development"),
    ("R016", "Barbara Clark", "Performance Engineer", "Senior", 145.0, 75.0, "2024-05-01", "2024-12-31", "US", "Quality"),
    ("R017", "Daniel Lewis", "Security Engineer", "Senior", 155.0, 50.0, "2024-03-01", "2024-06-30", "US", "Infrastructure"),
    ("R018", "Nancy Robinson", "QA Engineer", "Mid", 110.0, 100.0, "2024-07-01", "2024-12-31", "India", "Quality"),
    ("R019", "Matthew Walker", "Data Engineer", "Mid", 125.0, 100.0, "2024-07-15", "2024-12-31", "India", "Development"),
    ("R020", "Karen Young", "Data Engineer", "Mid", 125.0, 100.0, "2024-10-01", "2024-12-31", "India", "Development"),
    ("R021", "Steven Hall", "Technical Writer", "Mid", 100.0, 75.0, "2024-01-15", "2024-12-31", "India", "Documentation"),
    ("R022", "Betty Allen", "Business Analyst", "Senior", 130.0, 50.0, "2024-01-15", "2024-06-30", "US", "Management"),
    ("R023", "Paul King", "Data Engineer", "Senior", 140.0, 100.0, "2024-10-01", "2024-12-31", "India", "Development"),
    ("R024", "Sandra Wright", "Support Engineer", "Mid", 115.0, 100.0, "2024-12-01", "2024-12-31", "India", "Support"),
    ("R025", "Kevin Scott", "Support Engineer", "Mid", 115.0, 100.0, "2024-12-01", "2024-12-31", "India", "Support")
]

# Create Resource DataFrame
df_resources = spark.createDataFrame(resource_data, resource_schema)

# Dependencies Schema
dependency_schema = StructType([
    StructField("dependency_id", StringType(), False),
    StructField("predecessor_task", StringType(), False),
    StructField("successor_task", StringType(), False),
    StructField("dependency_type", StringType(), False),
    StructField("lag_days", IntegerType(), False)
])

# Calculate Critical Path
def calculate_critical_path(wbs_df):
    """
    Calculate and identify critical path tasks
    """
    window_spec = Window.partitionBy("phase").orderBy("start_date")
    
    critical_path_df = wbs_df.withColumn(
        "duration_days",
        datediff(col("end_date"), col("start_date"))
    ).withColumn(
        "float_days",
        when(col("critical_path"), lit(0)).otherwise(lit(5))
    ).withColumn(
        "early_start", col("start_date")
    ).withColumn(
        "early_finish", col("end_date")
    ).withColumn(
        "late_start",
        date_add(col("start_date"), col("float_days"))
    ).withColumn(
        "late_finish",
        date_add(col("end_date"), col("float_days"))
    )
    
    return critical_path_df

df_critical_path = calculate_critical_path(df_wbs)

# Success Metrics and KPIs Schema
kpi_schema = StructType([
    StructField("kpi_id", StringType(), False),
    StructField("kpi_category", StringType(), False),
    StructField("kpi_name", StringType(), False),
    StructField("kpi_description", StringType(), False),
    StructField("target_value", DoubleType(), False),
    StructField("baseline_value", DoubleType(), True),
    StructField("current_value", DoubleType(), True),
    StructField("unit", StringType(), False),
    StructField("measurement_frequency", StringType(), False),
    StructField("owner", StringType(), False)
])

# Success Metrics Data
kpi_data = [
    ("KPI001", "Schedule", "Schedule Performance Index (SPI)", "Ratio of earned value to planned value", 1.0, 1.0, 0.95, "Ratio", "Weekly", "Project Manager"),
    ("KPI002", "Cost", "Cost Performance Index (CPI)", "Ratio of earned value to actual cost", 1.0, 1.0, 1.02, "Ratio", "Weekly", "Project Manager"),
    ("KPI003", "Quality", "Defect Density", "Number of defects per 1000 lines of code", 2.0, 5.0, 4.2, "Defects/KLOC", "Sprint", "QA Lead"),
    ("KPI004", "Quality", "Test Coverage", "Percentage of code covered by automated tests", 85.0, 0.0, 72.0, "Percentage", "Sprint", "QA Lead"),
    ("KPI005", "Performance", "Job Success Rate", "Percentage of successful PySpark job executions", 99.0, 95.0, 96.5, "Percentage", "Daily", "Technical Lead"),
    ("KPI006", "Performance", "Average Job Runtime", "Average execution time improvement vs Informatica", -30.0, 0.0, -15.0, "Percentage", "Weekly", "Performance Engineer"),
    ("KPI007", "Scope", "Migration Progress", "Percentage of mappings migrated", 100.0, 0.0, 18.0, "Percentage", "Weekly", "Technical Lead"),
    ("KPI008", "Resource", "Team Velocity", "Story points completed per sprint", 80.0, 65.0, 75.0, "Points", "Sprint", "Scrum Master"),
    ("KPI009", "Quality", "Data Quality Score", "Percentage of data quality checks passed", 99.5, 98.0, 98.5, "Percentage", "Daily", "Data Architect"),
    ("KPI010", "Cost", "Budget Variance", "Percentage variance from planned budget", 0.0, 0.0, 2.5, "Percentage", "Monthly", "Project Manager"),
    ("KPI011", "Satisfaction", "Stakeholder Satisfaction", "Stakeholder satisfaction survey score", 4.5, 4.0, 4.2, "Score (1-5)", "Monthly", "Project Manager"),
    ("KPI012", "Risk", "Risk Mitigation Effectiveness", "Percentage of risks successfully mitigated", 90.0, 0.0, 85.0, "Percentage", "Monthly", "Project Manager"),
    ("KPI013", "Productivity", "Code Reuse Ratio", "Percentage of code leveraging common frameworks", 70.0, 0.0, 65.0, "Percentage", "Sprint", "Technical Lead"),
    ("KPI014", "Quality", "Production Incidents", "Number of P1/P2 incidents post-deployment", 2.0, 10.0, 0.0, "Count", "Weekly", "DevOps Lead"),
    ("KPI015", "Performance", "Resource Utilization", "Percentage of allocated resources actively utilized", 90.0, 0.0, 88.0, "Percentage", "Weekly", "Resource Manager")
]

# Create KPI DataFrame
df_kpis = spark.createDataFrame(kpi_data, kpi_schema)

# Risk Register Schema
risk_schema = StructType([
    StructField("risk_id", StringType(), False),
    StructField("risk_category", StringType(), False),
    StructField("risk_description", StringType(), False),
    StructField("probability", StringType(), False),
    StructField("impact", StringType(), False),
    StructField("risk_score", IntegerType(), False),
    StructField("mitigation_strategy", StringType(), False),
    StructField("owner", StringType(), False),
    StructField("status", StringType(), False)
])

# Risk Data
risk_data = [
    ("RSK001", "Technical", "PySpark skills gap in existing team", "High", "High", 9, "Provide comprehensive training and hire experienced PySpark developers", "Technical Lead", "Active"),
    ("RSK002", "Schedule", "Underestimation of complex mapping conversion effort", "Medium", "High", 6, "Build buffer time and conduct detailed effort estimation workshops", "Project Manager", "Active"),
    ("RSK003", "Quality", "Data quality issues in migrated workflows", "Medium", "High", 6, "Implement robust data validation framework and reconciliation processes", "QA Lead", "Active"),
    ("RSK004", "Technical", "Performance degradation vs Informatica baseline", "Medium", "Medium", 4, "Conduct performance testing early and optimize continuously", "Performance Engineer", "Active"),
    ("RSK005", "Resource", "Key resource attrition during project", "Low", "High", 3, "Cross-train team members and maintain knowledge repository", "Resource Manager", "Active"),
    ("RSK006", "Business", "Scope creep and changing requirements", "Medium", "Medium", 4, "Establish strong change control process and governance", "Project Manager", "Active"),
    ("RSK007", "Technical", "Integration challenges with downstream systems", "Medium", "High", 6, "Early integration testing and API contract validation", "Integration Lead", "Active"),
    ("RSK008", "Infrastructure", "Cloud infrastructure availability issues", "Low", "Medium", 2, "Design for high availability and implement disaster recovery", "DevOps Lead", "Mitigated"),
    ("RSK009", "Compliance", "Data governance and security compliance gaps", "Low", "High", 3, "Engage security team early and conduct compliance audits", "Security Engineer", "Mitigated"),
    ("RSK010", "Business", "Stakeholder resistance to change", "Medium", "Medium", 4, "Robust change management and communication plan", "Change Manager", "Active")
]

# Create Risk DataFrame
df_risks = spark.createDataFrame(risk_data, risk_schema)

# Budget Allocation Schema
budget_schema = StructType([
    StructField("budget_category", StringType(), False),
    StructField("budget_subcategory", StringType(), False),
    StructField("planned_amount", DoubleType(), False),
    StructField("actual_amount", DoubleType(), False),
    StructField("committed_amount", DoubleType(), False),
    StructField("forecast_amount", DoubleType(), False),
    StructField("variance_amount", DoubleType(), False),
    StructField("variance_percentage", DoubleType(), False)
])

# Budget Data
budget_data = [
    ("Labor", "Internal FTE", 1200000.0, 180000.0, 400000.0, 1220000.0, -20000.0, -1.67),
    ("Labor", "External Contractors", 600000.0, 85000.0, 200000.0, 590000.0, 10000.0, 1.67),
    ("Infrastructure", "Cloud Computing (Databricks)", 350000.0, 42000.0, 120000.0, 345000.0, 5000.0, 1.43),
    ("Infrastructure", "Storage (Data Lake)", 100000.0, 12000.0, 35000.0, 98000.0, 2000.0, 2.0),
    ("Software", "Development Tools", 80000.0, 15000.0, 40000.0, 80000.0, 0.0, 0.0),
    ("Software", "Testing Tools", 50000.0, 8000.0, 25000.0, 50000.0, 0.0, 0.0),
    ("Software", "Project Management Tools", 30000.0, 5000.0, 15000.0, 30000.0, 0.0, 0.0),
    ("Training", "PySpark Training Programs", 60000.0, 12000.0, 25000.0, 58000.0, 2000.0, 3.33),
    ("Training", "Cloud Platform Certifications", 30000.0, 4000.0, 12000.0, 29000.0, 1000.0, 3.33),
    ("Contingency", "Risk Reserve", 200000.0, 0.0, 0.0, 180000.0, 20000.0, 10.0)
]

# Create Budget DataFrame
df_budget = spark.createDataFrame(budget_data, budget_schema)

# Project Analytics and Reporting Functions

def calculate_project_metrics(wbs_df, resources_df, budget_df):
    """
    Calculate comprehensive project metrics
    """
    # Total Planned Hours
    total_hours = wbs_df.agg(sum("estimated_hours").alias("total_planned_hours"))
    
    # Completed Hours
    completed_hours = wbs_df.filter(col("status") == "Completed") \
        .agg(sum("estimated_hours").alias("completed_hours"))
    
    # In Progress Hours
    in_progress_hours = wbs_df.filter(col("status") == "In Progress") \
        .agg((sum("estimated_hours") * avg("completion_percentage") / 100).alias("in_progress_hours"))
    
    # Budget Summary
    budget_summary = budget_df.agg(
        sum("planned_amount").alias("total_planned_budget"),
        sum("actual_amount").alias("total_actual_spend"),
        sum("forecast_amount").alias("total_forecast"),
        sum("variance_amount").alias("total_variance")
    )
    
    # Resource Count
    resource_count = resources_df.select(count("*").alias("total_resources"))
    
    # Critical Path Tasks
    critical_tasks = wbs_df.filter(col("critical_path") == True) \
        .select(count("*").alias("critical_task_count"))
    
    return {
        "hours": total_hours.union(completed_hours).union(in_progress_hours),
        "budget": budget_summary,
        "resources": resource_count,
        "critical": critical_tasks
    }

def generate_phase_summary