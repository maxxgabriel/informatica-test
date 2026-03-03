"""
PySpark Target Architecture and Migration Strategy - Design Document as Code
===============================================================================
This module serves as executable documentation for the target PySpark architecture,
migration strategy, and implementation standards for Informatica to PySpark migration.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from enum import Enum
from datetime import datetime


# ==============================================================================
# ARCHITECTURE COMPONENTS
# ==============================================================================

class OrchestrationType(Enum):
    """Supported orchestration platforms"""
    AIRFLOW = "Apache Airflow"
    DATABRICKS_WORKFLOWS = "Databricks Workflows"
    AWS_STEP_FUNCTIONS = "AWS Step Functions"
    AZURE_DATA_FACTORY = "Azure Data Factory"


class StorageType(Enum):
    """Data storage layer types"""
    S3 = "AWS S3"
    ADLS = "Azure Data Lake Storage"
    GCS = "Google Cloud Storage"
    DELTA_LAKE = "Delta Lake"
    ICEBERG = "Apache Iceberg"


class MigrationApproach(Enum):
    """Migration strategies"""
    LIFT_AND_SHIFT = "Lift and Shift"
    REFACTOR = "Refactor"
    HYBRID = "Hybrid Approach"


@dataclass
class PySparkJobArchitecture:
    """
    Standard PySpark Job Architecture Design
    
    Directory Structure:
    project_root/
    ├── config/
    │   ├── dev.yaml
    │   ├── uat.yaml
    │   └── prod.yaml
    ├── jobs/
    │   ├── bronze_ingestion/
    │   ├── silver_transformation/
    │   └── gold_aggregation/
    ├── common/
    │   ├── utils/
    │   ├── transformations/
    │   └── validators/
    ├── tests/
    │   ├── unit/
    │   ├── integration/
    │   └── e2e/
    ├── orchestration/
    │   ├── airflow_dags/
    │   └── databricks_workflows/
    └── monitoring/
        ├── logging/
        └── metrics/
    """
    
    job_name: str
    job_type: str  # bronze/silver/gold
    layers: List[str] = field(default_factory=lambda: ["ingestion", "transformation", "validation", "publishing"])
    
    @staticmethod
    def get_standard_structure():
        return {
            "config_management": "YAML-based configuration with environment overrides",
            "code_organization": "Modular design with reusable components",
            "naming_convention": "snake_case for files, PascalCase for classes",
            "package_structure": "Layered architecture (bronze/silver/gold)",
        }


@dataclass
class InfrastructureRequirements:
    """Infrastructure specifications for PySpark deployment"""
    
    compute_platform: str  # Databricks, EMR, Synapse, etc.
    cluster_config: Dict[str, any] = field(default_factory=dict)
    
    def get_compute_specifications(self):
        return {
            "development": {
                "driver_node": "4 cores, 16GB RAM",
                "worker_nodes": "2-8 autoscaling, 4 cores, 16GB RAM each",
                "spark_version": "3.5.0",
                "python_version": "3.10"
            },
            "production": {
                "driver_node": "8 cores, 32GB RAM",
                "worker_nodes": "4-20 autoscaling, 8 cores, 32GB RAM each",
                "spark_version": "3.5.0",
                "python_version": "3.10",
                "autoscaling_enabled": True,
                "spot_instances": "80% spot, 20% on-demand"
            }
        }
    
    def get_network_requirements(self):
        return {
            "vpc_configuration": "Private subnet with NAT gateway",
            "security_groups": "Restricted ingress/egress rules",
            "vpc_peering": "Required for database connectivity",
            "endpoints": ["S3", "DynamoDB", "Secrets Manager"]
        }


@dataclass
class DataStorageStrategy:
    """Data lake architecture and storage design"""
    
    storage_platform: StorageType
    table_format: str  # Delta, Iceberg, Parquet
    
    def get_medallion_architecture(self):
        return {
            "bronze": {
                "description": "Raw ingestion layer",
                "path": "s3://datalake/bronze/",
                "format": "delta",
                "partitioning": "ingestion_date",
                "retention": "2 years",
                "compression": "snappy"
            },
            "silver": {
                "description": "Cleansed and conformed layer",
                "path": "s3://datalake/silver/",
                "format": "delta",
                "partitioning": "business_date, source_system",
                "retention": "5 years",
                "compression": "snappy",
                "optimizations": ["z-order", "bloom filters"]
            },
            "gold": {
                "description": "Business-ready aggregated layer",
                "path": "s3://datalake/gold/",
                "format": "delta",
                "partitioning": "report_date",
                "retention": "7 years",
                "compression": "snappy",
                "optimizations": ["z-order", "data skipping"]
            }
        }
    
    def get_governance_controls(self):
        return {
            "access_control": "Lake Formation / Unity Catalog",
            "data_catalog": "AWS Glue / Databricks Unity Catalog",
            "encryption": {
                "at_rest": "AES-256",
                "in_transit": "TLS 1.2+"
            },
            "audit_logging": "CloudTrail / Audit Logs",
            "data_quality": "Great Expectations / Deequ"
        }


@dataclass
class LoggingMonitoringFramework:
    """Centralized logging and monitoring design"""
    
    logging_platform: str
    monitoring_platform: str
    
    def get_logging_configuration(self):
        return {
            "log_levels": {
                "development": "DEBUG",
                "uat": "INFO",
                "production": "WARN"
            },
            "log_destinations": [
                "CloudWatch Logs",
                "S3 (long-term storage)",
                "Splunk/ELK (analysis)"
            ],
            "structured_logging": True,
            "log_format": "JSON",
            "required_fields": [
                "timestamp",
                "job_name",
                "run_id",
                "environment",
                "log_level",
                "message",
                "metadata"
            ]
        }
    
    def get_monitoring_metrics(self):
        return {
            "job_metrics": [
                "execution_duration",
                "records_processed",
                "data_quality_score",
                "success_failure_rate"
            ],
            "infrastructure_metrics": [
                "cpu_utilization",
                "memory_usage",
                "disk_io",
                "network_throughput"
            ],
            "business_metrics": [
                "data_freshness",
                "sla_compliance",
                "cost_per_job",
                "data_volume_trends"
            ],
            "alerting": {
                "channels": ["PagerDuty", "Slack", "Email"],
                "thresholds": {
                    "job_failure": "immediate",
                    "sla_breach": "within 15 minutes",
                    "data_quality_issue": "within 30 minutes"
                }
            }
        }


@dataclass
class ErrorHandlingStrategy:
    """Error handling and retry mechanism design"""
    
    retry_policy: Dict[str, any] = field(default_factory=dict)
    
    def get_error_handling_patterns(self):
        return {
            "transient_errors": {
                "examples": ["Network timeout", "Resource unavailable"],
                "strategy": "Exponential backoff retry",
                "max_retries": 3,
                "initial_delay": "30 seconds",
                "backoff_multiplier": 2
            },
            "data_quality_errors": {
                "examples": ["Null values", "Schema mismatch"],
                "strategy": "Quarantine bad records",
                "action": "Continue processing, alert data owners"
            },
            "fatal_errors": {
                "examples": ["Code bug", "Permission denied"],
                "strategy": "Fail fast",
                "action": "Immediate alert, manual intervention required"
            }
        }
    
    def get_circuit_breaker_config(self):
        return {
            "enabled": True,
            "failure_threshold": 5,
            "timeout": "60 seconds",
            "reset_timeout": "300 seconds",
            "monitoring": "Track open/closed state transitions"
        }


@dataclass
class ConfigurationManagement:
    """Configuration management approach"""
    
    config_source: str
    
    def get_config_hierarchy(self):
        return {
            "priority_order": [
                "1. Runtime parameters (highest)",
                "2. Environment variables",
                "3. Environment-specific YAML",
                "4. Default configuration (lowest)"
            ],
            "config_categories": {
                "connection_configs": "Database, API endpoints",
                "job_configs": "Parallelism, memory settings",
                "business_configs": "Date ranges, thresholds",
                "infrastructure_configs": "Cluster size, autoscaling"
            },
            "secret_management": {
                "platform": "AWS Secrets Manager / Azure Key Vault",
                "rotation": "90 days",
                "access_control": "IAM roles / Managed identities"
            }
        }
    
    def get_config_template(self):
        return """
# config/prod.yaml
environment: production

spark:
  app_name: ${job_name}_prod
  configs:
    spark.sql.adaptive.enabled: true
    spark.sql.adaptive.coalescePartitions.enabled: true
    spark.sql.shuffle.partitions: 200
    spark.dynamicAllocation.enabled: true

sources:
  database:
    jdbc_url: ${DB_JDBC_URL}
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD_SECRET}
    connection_pool_size: 10

storage:
  bronze_path: s3://prod-datalake/bronze
  silver_path: s3://prod-datalake/silver
  gold_path: s3://prod-datalake/gold
  checkpoint_location: s3://prod-datalake/checkpoints

data_quality:
  enabled: true
  fail_on_error: false
  quarantine_path: s3://prod-datalake/quarantine

monitoring:
  log_level: WARN
  metrics_enabled: true
  cloudwatch_namespace: DataPlatform/PySpark
"""


@dataclass
class TestingValidationStrategy:
    """Comprehensive testing strategy"""
    
    coverage_target: float = 80.0
    
    def get_testing_levels(self):
        return {
            "unit_tests": {
                "framework": "pytest",
                "scope": "Individual functions and transformations",
                "coverage_target": "90%",
                "execution": "Pre-commit hook, CI/CD",
                "tools": ["pytest-spark", "chispa"]
            },
            "integration_tests": {
                "framework": "pytest",
                "scope": "End-to-end job execution with sample data",
                "coverage_target": "80%",
                "execution": "CI/CD pipeline",
                "test_data": "Synthetic datasets covering edge cases"
            },
            "data_quality_tests": {
                "framework": "Great Expectations",
                "scope": "Data validation at each layer",
                "rules": [
                    "Schema validation",
                    "Completeness checks",
                    "Referential integrity",
                    "Business rule validation"
                ]
            },
            "performance_tests": {
                "framework": "Custom benchmarking",
                "scope": "Job execution time, resource utilization",
                "baseline": "Informatica performance metrics",
                "acceptance": "Within 10% of baseline"
            },
            "regression_tests": {
                "scope": "Output comparison with Informatica",
                "strategy": "Parallel run validation",
                "tolerance": "Row-level match 99.9%"
            }
        }
    
    def get_validation_approach(self):
        return {
            "pre_migration_validation": {
                "tasks": [
                    "Source data profiling",
                    "Informatica logic documentation",
                    "Expected output capture"
                ]
            },
            "parallel_run_validation": {
                "duration": "2-4 weeks per workload",
                "comparison": "Row-by-row, column-by-column",
                "tools": ["custom reconciliation scripts", "data diff tools"],
                "sign_off_criteria": "99.9% match rate"
            },
            "post_migration_validation": {
                "monitoring_period": "30 days",
                "checks": [
                    "Data quality scores",
                    "SLA compliance",
                    "Error rates",
                    "Performance benchmarks"
                ]
            }
        }


@dataclass
class MigrationStrategy:
    """Phased migration approach and rollout plan"""
    
    approach: MigrationApproach
    
    def get_migration_phases(self):
        return {
            "phase_1_foundation": {
                "duration": "4-6 weeks",
                "objectives": [
                    "Set up infrastructure",
                    "Establish CI/CD pipelines",
                    "Implement logging/monitoring framework",
                    "Create reusable libraries"
                ],
                "deliverables": [
                    "Infrastructure as Code templates",
                    "Common utilities library",
                    "CI/CD pipeline operational",
                    "Monitoring dashboards"
                ]
            },
            "phase_2_pilot": {
                "duration": "6-8 weeks",
                "objectives": [
                    "Migrate 3-5 low complexity jobs",
                    "Validate migration patterns",
                    "Refine testing strategy",
                    "Train team"
                ],
                "deliverables": [
                    "Pilot jobs in production",
                    "Migration runbook",
                    "Lessons learned document",
                    "Team certified on PySpark"
                ]
            },
            "phase_3_wave_migration": {
                "duration": "12-24 weeks",
                "objectives": [
                    "Migrate jobs in priority order",
                    "Parallel run validation",
                    "Performance optimization",
                    "User acceptance testing"
                ],
                "approach": "Agile sprints (2-week cycles)",
                "capacity": "5-10 jobs per sprint",
                "deliverables": [
                    "Migrated jobs in production",
                    "Validation reports",
                    "Performance benchmarks",
                    "Sign-off from business"
                ]
            },
            "phase_4_decommission": {
                "duration": "4-8 weeks",
                "objectives": [
                    "Stabilize PySpark environment",
                    "Decommission Informatica",
                    "Knowledge transfer",
                    "Hypercare support"
                ],
                "deliverables": [
                    "Informatica shutdown",
                    "Complete documentation",
                    "Support team trained",
                    "Post-migration review"
                ]
            }
        }
    
    def get_prioritization_criteria(self):
        return {
            "priority_1_high": {
                "characteristics": [
                    "Low complexity",
                    "Well-documented",
                    "Low business criticality",
                    "Suitable for pilot"
                ],
                "examples": ["Simple file to table loads", "Lookup transformations"]
            },
            "priority_2_medium": {
                "characteristics": [
                    "Moderate complexity",
                    "Standard transformations",
                    "Medium business criticality",
                    "Good candidate for patterns"
                ],
                "examples": ["Multi-source joins", "Aggregations", "SCD Type 2"]
            },
            "priority_3_low": {
                "characteristics": [
                    "High complexity",
                    "Custom logic",
                    "High business criticality",
                    "Requires deep analysis"
                ],
                "examples": ["Complex business rules", "Real-time streaming", "ML pipelines"]
            }
        }
    
    def get_rollback_procedures(self):
        return {
            "rollback_triggers": [
                "Critical production issue",
                "Data quality failure",
                "Performance degradation > 50%",
                "Business stakeholder request"
            ],
            "rollback_steps": {
                "immediate": [
                    "1. Stop PySpark job execution",
                    "2. Activate Informatica job",
                    "3. Notify stakeholders",
                    "4. Monitor Informatica execution"
                ],
                "post_rollback": [
                    "1. Conduct root cause analysis",
                    "2. Document issues and fixes",
                    "3. Update code and tests",
                    "4. Plan re-migration"
                ]
            },
            "rollback_automation": {
                "infrastructure": "IaC allows quick environment teardown",
                "orchestration": "Switch DAG activation in Airflow",
                "data": "Delta Lake time travel to previous version",
                "monitoring": "Automatic alerts on rollback events"
            }
        }


@dataclass
class OrchestrationDesign:
    """Orchestration platform design and DAG patterns"""
    
    platform: OrchestrationType
    
    def get_airflow_design(self):
        return """
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30)
}

dag = DAG(
    'customer_data_pipeline',
    default_args=default_args,
    description='Migrated from Informatica workflow WF_CUSTOMER_DAILY',
    schedule_interval='0 2 * * *',  # 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['migration', 'customer', 'daily'],
    max_active_runs=1
)

# Bronze layer ingestion
bronze_ingestion = DatabricksSubmitRunOperator(
    task_id='bronze_customer_ingestion',
    databricks_conn_id='databricks_prod',
    json={
        'new_cluster': {
            'spark_version': '13.3.x-scala2.12',
            'node_type_id': 'i3.xlarge',
            'num_workers': 4,
            'autoscale': {'min_workers': 2, 'max_workers': 8}
        },
        'spark_python_task': {
            'python_file': 's3://code-bucket/jobs/bronze/customer_ingestion.py',
            'parameters': [
                '--environment', 'prod',
                '--run_date', '{{ ds }}'
            ]
        }
    },
    dag=dag
)

# Silver layer transformation
silver_transformation = DatabricksSubmitRunOperator(
    task_id='silver_customer_transformation',
    databricks_conn_id='databricks_prod',
    json={
        'new_cluster': {
            'spark_version': '13.3.x-scala2.12',
            'node_type_id': 'i3.xlarge',
            'num_workers': 4,
            'autoscale': {'min_workers': 2, 'max_workers': 8}
        },
        'spark_python_task': {
            'python_file': 's3://code-bucket/jobs/silver/customer_transformation.py',
            'parameters': [
                '--environment', 'prod',
                '--run_date', '{{ ds }}'
            ]
        }
    },
    dag=dag
)

# Gold layer aggregation
gold_aggregation = DatabricksSubmitRunOperator(
    task_id='gold_customer_aggregation',
    databricks_conn_id='databricks_prod',
    json={
        'new_cluster': {
            'spark_version': '13.3.x-scala2.12',
            'node_type_id': 'i3.xlarge',
            'num_workers': 2,
            'autoscale': {'min_workers': 2, 'max_workers': 4}
        },
        'spark_python_task': {
            'python_file': 's3://code-bucket/jobs/gold/customer_aggregation.py',
            'parameters': [
                '--environment', 'prod',
                '--run_date', '{{ ds }}'
            ]
        }
    },
    dag=dag
)

# Data quality validation
def validate_data_quality(**context):
    from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
    # Validation logic
    pass

data_quality_check = PythonOperator(
    task_id='data_quality_validation',
    python_callable=validate_data_quality,
    provide_context=True,
    dag=dag
)

# Define dependencies
bronze_ingestion >> silver_transformation >> gold_aggregation >> data_quality_check
"""
    
    def get_databricks_workflow_design(self):
        return """
# Databricks Workflow JSON definition
{
  "name": "customer_data_pipeline",
  "email_notifications": {
    "on_failure": ["data-team@company.com"],
    "no_alert_for_skipped_runs": false
  },
  "timeout_seconds": 7200,
  "max_concurrent_runs": 1,
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "America/New_York",
    "pause_status": "UNPAUSED"
  },
  "tasks": [
    {
      "task_key": "bronze_customer_ingestion",
      "description": "Ingest customer data to bronze layer",
      "timeout_seconds": 3600,
      "max_retries": 3,
      "min_retry_interval_millis": 300000,
      "retry_on_timeout": true,
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "autoscale": {
          "min_workers": 2,
          "max_workers": 8
        }
      },
      "spark_python_task": {
        "python_file": "dbfs:/jobs/bronze/customer_ingestion.py",
        "parameters": [
          "--environment", "prod",
          "--run_date", "{{job.start_time.date}}"
        ]
      }
    },
    {
      "task_key": "silver_customer_transformation",
      "depends_on": [{"task_key": "bronze_customer_ingestion"}],
      "timeout_seconds": 3600,
      "max_retries": 3,
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "autoscale": {
          "min_workers": 2,
          "max_workers": 8
        }
      },
      "spark_python_task": {
        "python_file": "dbfs:/jobs/silver/customer_transformation.py",
        "parameters": [
          "--environment", "prod",
          "--run_date", "{{job.start_time.date}}"
        ]
      }
    },
    {
      "task_key": "gold_customer_aggregation",
      "depends_on": [{"task_key": "silver_customer_transformation"}],
      "timeout_seconds": 1800,
      "max_retries": 3,
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "autoscale": {
          "min_workers": 2,
          "max_workers": 4
        }
      },
      "spark_python_task": {
        "python_file": "dbfs:/jobs/gold/customer_aggregation.py",
        "parameters": [
          "--environment", "prod",
          "--run_date", "{{job.start_time.date}}"
        ]
      }
    }
  ]
}
"""


# ==============================================================================
# REUSABLE CODE TEMPLATES
# ==============================================================================

class PySparkJobTemplate:
    """Standard PySpark job template with best practices"""
    
    @staticmethod
    def get_base_job_template():
        return '''
"""
Job: {job_name}
Description: {job_description}
Migrated from: Informatica workflow {informatica_workflow}
Author: Data Engineering Team
Date: {migration_date}
"""

import sys
import logging
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Import common utilities
from common.utils.config_loader import ConfigLoader
from common.utils.logger import setup_logging
from common.utils.spark_factory import SparkFactory
from common.utils.exception_handler import handle_exceptions
from common.transformations.base_transformer import BaseTransformer
from common.validators.data_quality import DataQualityValidator


class {JobClassName}(BaseTransformer):
    """
    {Job Description}
    
    Source System: {source_system}
    Target Layer: {target_layer}
    Frequency: {schedule}
    SLA: {sla_requirement}
    """
    
    def __init__(self, config: Dict[str, Any], spark: SparkSession):
        """
        Initialize job with configuration and Spark session
        
        Args:
            config: Job configuration dictionary
            spark: Active Spark session
        """
        super().__init__(config, spark)
        self.job_name = config['job']['name']
        self.environment = config['environment']
        self.run_date = config['run_date']
        
        # Set up logging
        self.logger = logging.getLogger(self.job_name)
        
        # Initialize metrics
        self.metrics = {
            'start_time': datetime.now(),
            'records_read': 0,
            'records_written': 0,
            'records_rejected': 0
        }
    
    @handle_exceptions(default_return_value=False)
    def extract(self) -> DataFrame:
        """
        Extract data from source system
        
        Returns:
            DataFrame: Raw source data
        """
        self.logger.info(f"Starting extraction for {self.job_name}")
        
        # Example: Read from JDBC source
        source_df = (
            self.spark.read
            .format("jdbc")
            .option("url", self.config['source']['jdbc_url'])
            .option("dbtable", self.config['source']['table'])
            .option("user", self.config['source']['username'])
            .option("password", self.config['source']['password'])
            .option("fetchsize", 10000)
            .option("numPartitions", 8)
            .load()
        )
        
        self.metrics['records_read'] = source_df.count()
        self.logger.info(f"Extracted {self.metrics['records_read']} records")
        
        return source_df
    
    @handle_exceptions(default_return_value=None)
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply business transformations
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Transformed data
        """
        self.logger.info("Starting transformation")
        
        # Add audit columns
        transformed_df = (
            df
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("job_run_id", F.lit(self.config['run_id']))
            .withColumn("source_system", F.lit(self.config['source']['system']))
        )
        
        # Apply business logic
        # TODO: Implement specific transformations migrated from Informatica
        
        self.logger.info("Transformation completed")
        return transformed_df
    
    @handle_exceptions(default_return_value=False)
    def validate(self, df: DataFrame) -> bool:
        """
        Validate data quality
        
        Args:
            df: DataFrame to validate
            
        Returns:
            bool: True if validation passes
        """
        self.logger.info("Starting data quality validation")
        
        validator = DataQualityValidator(self.config, self.spark)
        
        validation_rules = [
            validator.check_null_values(df, ['primary_key_column']),
            validator.check_duplicates(df, ['primary_key_column']),
            validator.check_referential_integrity(df, self.config['validation']['reference_tables'])
        ]
        
        validation_passed = all(validation_rules)
        
        if not validation_passed:
            self.logger.error("Data quality validation failed")
            self.metrics['records_rejected'] = df.count()
        
        return validation_passed
    
    @handle_exceptions(default_return_value=False)
    def load(self, df: DataFrame) -> bool:
        """
        Load data to target location
        
        Args:
            df: DataFrame to load
            
        Returns:
            bool: True if load successful
        """
        self.logger.info("Starting data load")
        
        target_path = self.config['target']['path']
        
        # Write to Delta Lake with merge
        if DeltaTable.isDeltaTable(self.spark, target_path):
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Perform merge (upsert) operation
            (
                delta_table.alias("target")
                .merge(
                    df.alias("source"),
                    "target.primary_key = source.primary_key"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            # Initial load
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy(self.config['target']['partition_columns'])
                .option("overwriteSchema", "true")
                .save(target_path)
            )
        
        self.metrics['records_written'] = df.count()
        self.logger.info(f"Loaded {self.metrics['records_written']} records")
        
        return True
    
    def run(self) -> bool:
        """
        Execute complete ETL pipeline
        
        Returns:
            bool: True if job successful
        """
        try:
            self.logger.info(f"Starting job execution: {self.job_name}")
            
            # Extract
            source_df = self.extract()
            if source_df is None:
                raise Exception("Extraction failed")
            
            # Transform
            transformed_df = self.transform(source_df)
            if transformed_df is None:
                raise Exception("Transformation failed")
            
            # Validate
            if not self.validate(transformed_df):
                if self.config['data_quality']['fail_on_error']:
                    raise Exception("Data quality validation failed")
                else:
                    self.logger.warning("Data quality issues detected, continuing...")