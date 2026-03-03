"""
Migration Runbook and Documentation Generator
Comprehensive documentation framework for Informatica PowerCenter to PySpark migration
Author: Data Engineering Team
Version: 1.0.0
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
import logging
from typing import Dict, List, Any
from pathlib import Path
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MigrationDocumentationGenerator:
    """
    Generates comprehensive migration documentation and runbooks
    for Informatica PowerCenter to PySpark migration
    """
    
    def __init__(self, output_base_path: str):
        """
        Initialize documentation generator
        
        Args:
            output_base_path: Base directory for documentation output
        """
        self.output_base_path = Path(output_base_path)
        self.output_base_path.mkdir(parents=True, exist_ok=True)
        
        # Create documentation structure
        self.docs_structure = {
            'runbooks': self.output_base_path / 'runbooks',
            'standards': self.output_base_path / 'coding_standards',
            'mappings': self.output_base_path / 'transformation_mappings',
            'operations': self.output_base_path / 'operational_procedures',
            'troubleshooting': self.output_base_path / 'troubleshooting',
            'monitoring': self.output_base_path / 'monitoring',
            'training': self.output_base_path / 'training_materials',
            'knowledge_transfer': self.output_base_path / 'knowledge_transfer'
        }
        
        for path in self.docs_structure.values():
            path.mkdir(parents=True, exist_ok=True)
    
    def generate_migration_runbook(self) -> Dict[str, Any]:
        """Generate comprehensive migration runbook"""
        
        runbook = {
            "title": "Informatica PowerCenter to PySpark Migration Runbook",
            "version": "1.0.0",
            "last_updated": datetime.now().isoformat(),
            "sections": {
                "1_overview": {
                    "title": "Migration Overview",
                    "content": {
                        "purpose": "Guide for migrating ETL workflows from Informatica PowerCenter to PySpark",
                        "scope": "All ETL jobs, workflows, and data pipelines",
                        "timeline": "Phased migration approach with parallel runs",
                        "stakeholders": ["Data Engineering", "Data Analytics", "Business Intelligence", "Operations"]
                    }
                },
                "2_prerequisites": {
                    "title": "Prerequisites and Setup",
                    "steps": [
                        {
                            "step": "1",
                            "task": "Environment Setup",
                            "details": [
                                "Install Spark 3.x cluster",
                                "Configure AWS S3/ADLS for data storage",
                                "Setup metadata repository (PostgreSQL/MySQL)",
                                "Configure job orchestration (Airflow/Databricks)",
                                "Install monitoring tools (Grafana/Prometheus)"
                            ]
                        },
                        {
                            "step": "2",
                            "task": "Access and Permissions",
                            "details": [
                                "Source system database credentials",
                                "Target system access permissions",
                                "Cloud storage IAM roles",
                                "Repository access credentials",
                                "Monitoring dashboard access"
                            ]
                        },
                        {
                            "step": "3",
                            "task": "Code Repository Setup",
                            "details": [
                                "Create Git repository structure",
                                "Setup CI/CD pipelines",
                                "Configure code review process",
                                "Setup branch protection rules",
                                "Configure automated testing"
                            ]
                        }
                    ]
                },
                "3_migration_phases": {
                    "title": "Migration Execution Phases",
                    "phases": [
                        {
                            "phase": "1",
                            "name": "Discovery and Assessment",
                            "duration": "2-4 weeks",
                            "activities": [
                                "Inventory all PowerCenter objects",
                                "Analyze dependencies and data lineage",
                                "Identify complex transformations",
                                "Assess data volumes and performance",
                                "Document business rules and logic"
                            ],
                            "deliverables": [
                                "Source inventory spreadsheet",
                                "Dependency matrix",
                                "Complexity assessment report",
                                "Migration wave plan"
                            ]
                        },
                        {
                            "phase": "2",
                            "name": "Design and Development",
                            "duration": "8-12 weeks",
                            "activities": [
                                "Design PySpark job architecture",
                                "Develop transformation logic",
                                "Implement data quality checks",
                                "Create unit tests",
                                "Build reusable frameworks"
                            ],
                            "deliverables": [
                                "Technical design documents",
                                "PySpark code modules",
                                "Unit test suites",
                                "Configuration files"
                            ]
                        },
                        {
                            "phase": "3",
                            "name": "Testing and Validation",
                            "duration": "4-6 weeks",
                            "activities": [
                                "Execute unit tests",
                                "Perform integration testing",
                                "Conduct data reconciliation",
                                "Run performance testing",
                                "Execute UAT with business users"
                            ],
                            "deliverables": [
                                "Test results documentation",
                                "Reconciliation reports",
                                "Performance benchmarks",
                                "UAT sign-off"
                            ]
                        },
                        {
                            "phase": "4",
                            "name": "Parallel Run",
                            "duration": "2-4 weeks",
                            "activities": [
                                "Run old and new systems in parallel",
                                "Compare outputs and metrics",
                                "Monitor system performance",
                                "Validate data quality",
                                "Document discrepancies"
                            ],
                            "deliverables": [
                                "Parallel run reports",
                                "Issue log and resolutions",
                                "Performance comparison",
                                "Go-live approval"
                            ]
                        },
                        {
                            "phase": "5",
                            "name": "Cutover and Decommission",
                            "duration": "1-2 weeks",
                            "activities": [
                                "Execute cutover plan",
                                "Deactivate PowerCenter jobs",
                                "Monitor new system stability",
                                "Provide hypercare support",
                                "Archive old system artifacts"
                            ],
                            "deliverables": [
                                "Cutover checklist completed",
                                "Incident log",
                                "Lessons learned document",
                                "System handover"
                            ]
                        }
                    ]
                },
                "4_migration_checklist": {
                    "title": "Pre-Migration Checklist",
                    "categories": [
                        {
                            "category": "Source Analysis",
                            "items": [
                                {"item": "Extract PowerCenter XML metadata", "status": "Required"},
                                {"item": "Document source-to-target mappings", "status": "Required"},
                                {"item": "Identify lookup tables and reference data", "status": "Required"},
                                {"item": "Document parameter files and variables", "status": "Required"},
                                {"item": "Analyze workflow dependencies", "status": "Required"}
                            ]
                        },
                        {
                            "category": "Development Environment",
                            "items": [
                                {"item": "Spark cluster configured and tested", "status": "Required"},
                                {"item": "Development IDE setup complete", "status": "Required"},
                                {"item": "Database connections validated", "status": "Required"},
                                {"item": "Code repository initialized", "status": "Required"},
                                {"item": "CI/CD pipeline configured", "status": "Required"}
                            ]
                        },
                        {
                            "category": "Data Validation",
                            "items": [
                                {"item": "Reconciliation framework developed", "status": "Required"},
                                {"item": "Row count validation scripts", "status": "Required"},
                                {"item": "Column-level comparison tools", "status": "Required"},
                                {"item": "Data quality check framework", "status": "Required"},
                                {"item": "Null and duplicate checks", "status": "Required"}
                            ]
                        }
                    ]
                },
                "5_rollback_procedures": {
                    "title": "Rollback and Contingency Plans",
                    "scenarios": [
                        {
                            "scenario": "Critical Data Quality Issue",
                            "trigger": "Data reconciliation fails beyond threshold (>1% variance)",
                            "actions": [
                                "Stop PySpark jobs immediately",
                                "Reactivate PowerCenter workflows",
                                "Notify stakeholders and management",
                                "Document root cause analysis",
                                "Schedule remediation and retry"
                            ],
                            "decision_makers": ["Data Engineering Lead", "Data Architecture Manager"]
                        },
                        {
                            "scenario": "Performance Degradation",
                            "trigger": "Jobs exceed SLA by >50%",
                            "actions": [
                                "Review Spark job configurations",
                                "Analyze resource utilization",
                                "Implement performance optimizations",
                                "Consider reverting if unresolvable",
                                "Escalate to Spark expertise team"
                            ],
                            "decision_makers": ["Technical Lead", "Operations Manager"]
                        },
                        {
                            "scenario": "System Outage",
                            "trigger": "Spark cluster unavailable or infrastructure failure",
                            "actions": [
                                "Activate PowerCenter backup system",
                                "Execute manual recovery procedures",
                                "Coordinate with infrastructure team",
                                "Communicate with downstream consumers",
                                "Document incident timeline"
                            ],
                            "decision_makers": ["Incident Commander", "Operations Manager"]
                        }
                    ]
                }
            }
        }
        
        output_file = self.docs_structure['runbooks'] / 'migration_runbook.json'
        with open(output_file, 'w') as f:
            json.dump(runbook, f, indent=2)
        
        logger.info(f"Migration runbook generated: {output_file}")
        return runbook
    
    def generate_coding_standards(self) -> Dict[str, Any]:
        """Generate PySpark coding standards and best practices"""
        
        standards = {
            "title": "PySpark Coding Standards and Best Practices",
            "version": "1.0.0",
            "sections": {
                "1_naming_conventions": {
                    "title": "Naming Conventions",
                    "rules": {
                        "modules": {
                            "convention": "snake_case",
                            "examples": ["customer_transform.py", "data_quality_checks.py"],
                            "pattern": "^[a-z][a-z0-9_]*\\.py$"
                        },
                        "classes": {
                            "convention": "PascalCase",
                            "examples": ["DataQualityValidator", "ETLJobExecutor"],
                            "pattern": "^[A-Z][a-zA-Z0-9]*$"
                        },
                        "functions": {
                            "convention": "snake_case",
                            "examples": ["calculate_total_amount", "validate_data_quality"],
                            "pattern": "^[a-z][a-z0-9_]*$"
                        },
                        "variables": {
                            "convention": "snake_case",
                            "examples": ["customer_df", "transaction_count", "max_date"],
                            "pattern": "^[a-z][a-z0-9_]*$"
                        },
                        "constants": {
                            "convention": "UPPER_SNAKE_CASE",
                            "examples": ["MAX_RETRY_COUNT", "DEFAULT_PARTITION_SIZE"],
                            "pattern": "^[A-Z][A-Z0-9_]*$"
                        },
                        "dataframes": {
                            "convention": "descriptive_name_df",
                            "examples": ["customer_master_df", "sales_transaction_df"],
                            "suffix": "_df"
                        }
                    }
                },
                "2_code_structure": {
                    "title": "Code Organization",
                    "standards": {
                        "file_organization": {
                            "max_lines_per_file": 500,
                            "sections": [
                                "Imports (standard library, third-party, local)",
                                "Constants and configuration",
                                "Helper functions",
                                "Main business logic",
                                "Entry point (if __name__ == '__main__')"
                            ]
                        },
                        "function_design": {
                            "max_lines_per_function": 50,
                            "max_parameters": 5,
                            "single_responsibility": "Each function should do one thing well",
                            "return_types": "Always specify return type hints"
                        },
                        "class_design": {
                            "max_methods": 15,
                            "cohesion": "Related methods should be grouped together",
                            "encapsulation": "Use private methods (_method_name) for internal logic"
                        }
                    }
                },
                "3_documentation": {
                    "title": "Documentation Standards",
                    "requirements": {
                        "module_docstring": {
                            "required": True,
                            "format": "Google style",
                            "elements": ["Description", "Author", "Date", "Version"]
                        },
                        "function_docstring": {
                            "required": True,
                            "format": "Google style",
                            "elements": ["Description", "Args", "Returns", "Raises", "Example"]
                        },
                        "inline_comments": {
                            "when": "For complex logic or non-obvious code",
                            "style": "Explain WHY, not WHAT",
                            "max_line_length": 100
                        }
                    },
                    "example": '''
def transform_customer_data(customer_df: DataFrame, 
                           reference_df: DataFrame) -> DataFrame:
    """
    Transform customer data by applying business rules and enrichment.
    
    Args:
        customer_df: Source customer DataFrame with raw data
        reference_df: Reference data for lookup and enrichment
    
    Returns:
        DataFrame: Transformed customer data with all business rules applied
    
    Raises:
        ValueError: If required columns are missing
        DataQualityException: If data quality checks fail
    
    Example:
        >>> result_df = transform_customer_data(source_df, ref_df)
        >>> result_df.count()
        10000
    """
    # Implementation here
    pass
'''
                },
                "4_performance_optimization": {
                    "title": "Performance Best Practices",
                    "guidelines": [
                        {
                            "topic": "DataFrame Operations",
                            "rules": [
                                "Use DataFrame API over RDD when possible",
                                "Avoid collect() on large datasets",
                                "Use persist() for DataFrames used multiple times",
                                "Prefer filter() early in transformation chain",
                                "Use broadcast joins for small lookup tables (<100MB)"
                            ],
                            "example": '''
# Good: Filter early, broadcast small table
filtered_df = large_df.filter(col("status") == "active")
result_df = filtered_df.join(broadcast(small_lookup_df), "id")

# Bad: Collect large dataset, late filtering
data_list = large_df.collect()  # Avoid this!
result_df = large_df.join(small_df, "id").filter(col("status") == "active")
'''
                        },
                        {
                            "topic": "Partitioning",
                            "rules": [
                                "Partition data based on query patterns",
                                "Use repartition() for increasing partitions",
                                "Use coalesce() for decreasing partitions",
                                "Aim for 128MB-256MB per partition",
                                "Avoid skewed partitions"
                            ],
                            "example": '''
# Good: Proper partitioning strategy
df = df.repartition(100, "customer_id")
df.write.partitionBy("transaction_date").parquet(output_path)

# Bad: Single partition or excessive partitions
df = df.coalesce(1)  # Avoid single partition for large data
df = df.repartition(10000)  # Too many small partitions
'''
                        },
                        {
                            "topic": "Caching Strategy",
                            "rules": [
                                "Cache DataFrames used in multiple actions",
                                "Use appropriate storage level (MEMORY_AND_DISK)",
                                "Unpersist when no longer needed",
                                "Monitor cache memory usage"
                            ],
                            "example": '''
# Good: Strategic caching
customer_df = spark.read.parquet(source_path)
customer_df.persist(StorageLevel.MEMORY_AND_DISK)

# Use multiple times
result1 = customer_df.filter(col("status") == "active").count()
result2 = customer_df.groupBy("region").count()

# Clean up
customer_df.unpersist()
'''
                        }
                    ]
                },
                "5_error_handling": {
                    "title": "Error Handling Standards",
                    "patterns": [
                        {
                            "pattern": "Try-Except Blocks",
                            "usage": "Wrap external calls and risky operations",
                            "example": '''
try:
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=props)
    logger.info(f"Successfully read {df.count()} records from {table_name}")
except Exception as e:
    logger.error(f"Failed to read from {table_name}: {str(e)}")
    raise DataSourceException(f"Database read failed: {str(e)}")
'''
                        },
                        {
                            "pattern": "Custom Exceptions",
                            "usage": "Create domain-specific exceptions",
                            "example": '''
class DataQualityException(Exception):
    """Raised when data quality checks fail"""
    pass

class TransformationException(Exception):
    """Raised when transformation logic fails"""
    pass

def validate_data(df: DataFrame) -> None:
    null_count = df.filter(col("customer_id").isNull()).count()
    if null_count > 0:
        raise DataQualityException(f"Found {null_count} null customer IDs")
'''
                        },
                        {
                            "pattern": "Graceful Degradation",
                            "usage": "Handle failures with fallback logic",
                            "example": '''
def enrich_with_reference_data(df: DataFrame) -> DataFrame:
    try:
        reference_df = load_reference_data()
        return df.join(reference_df, "customer_id", "left")
    except Exception as e:
        logger.warning(f"Reference data unavailable: {e}. Continuing without enrichment.")
        return df  # Return original DataFrame
'''
                        }
                    ]
                },
                "6_testing_standards": {
                    "title": "Testing Requirements",
                    "levels": {
                        "unit_tests": {
                            "coverage_target": "80%",
                            "framework": "pytest",
                            "requirements": [
                                "Test each function independently",
                                "Use sample data for tests",
                                "Mock external dependencies",
                                "Test edge cases and error conditions"
                            ],
                            "example": '''
import pytest
from pyspark.sql import SparkSession
from transforms import calculate_total_amount

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[2]").getOrCreate()

def test_calculate_total_amount(spark):
    # Arrange
    data = [(1, 100.0, 2), (2, 50.0, 3)]
    df = spark.createDataFrame(data, ["id", "price", "quantity"])
    
    # Act
    result_df = calculate_total_amount(df)
    
    # Assert
    results = result_df.collect()
    assert results[0]["total_amount"] == 200.0
    assert results[1]["total_amount"] == 150.0
'''
                        },
                        "integration_tests": {
                            "scope": "End-to-end workflow testing",
                            "requirements": [
                                "Test with realistic data volumes",
                                "Validate data transformations",
                                "Check data quality rules",
                                "Verify output schema"
                            ]
                        },
                        "data_validation": {
                            "checks": [
                                "Row count reconciliation",
                                "Column-level aggregation comparison",
                                "Null value validation",
                                "Duplicate detection",
                                "Schema validation"
                            ]
                        }
                    }
                },
                "7_configuration_management": {
                    "title": "Configuration Standards",
                    "approach": {
                        "format": "YAML or JSON",
                        "storage": "Version controlled repository",
                        "environment_specific": "Separate configs for DEV/QA/PROD",
                        "secrets": "Use vault or parameter store, never hardcode"
                    },
                    "example": '''
# config/prod_config.yaml
job:
  name: customer_etl_job
  description: Extract and transform customer data
  
spark:
  app_name: customer_etl
  executor_memory: 8g
  executor_cores: 4
  driver_memory: 4g
  shuffle_partitions: 200
  
sources:
  customer_db:
    type: jdbc
    url: jdbc:postgresql://prod-db.company.com:5432/customer_db
    driver: org.postgresql.Driver
    table: customers
    
targets:
  customer_output:
    type: parquet
    path: s3://prod-bucket/customer/data
    mode: overwrite
    partition_by: ["region", "load_date"]
    
data_quality:
  null_check_columns: ["customer_id", "email", "created_date"]
  duplicate_check_keys: ["customer_id"]
  row_count_threshold: 0.05
'''
                },
                "8_logging_standards": {
                    "title": "Logging Best Practices",
                    "requirements": {
                        "log_levels": {
                            "DEBUG": "Detailed diagnostic information",
                            "INFO": "General informational messages",
                            "WARNING": "Warning messages for potentially harmful situations",
                            "ERROR": "Error events that might still allow continued execution",
                            "CRITICAL": "Severe errors causing premature termination"
                        },
                        "log_content": [
                            "Timestamp",
                            "Log level",
                            "Component/module name",
                            "Message",
                            "Contextual information (record counts, processing time)"
                        ],
                        "example": '''
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def process_data(df: DataFrame) -> DataFrame:
    start_time = datetime.now()
    input_count = df.count()
    logger.info(f"Starting data processing. Input records: {input_count}")
    
    try:
        # Transformation logic
        result_df = df.filter(col("status") == "active") \\
                      .withColumn("processed_date", current_date())
        
        output_count = result_df.count()
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Processing completed. Output records: {output_count}. "
                   f"Duration: {duration:.2f}s")
        return result_df
        
    except Exception as e:
        logger.error(f"Processing failed after {(datetime.now() - start_time).total_seconds():.2f}s: {str(e)}")
        raise
'''
                    }
                }
            }
        }
        
        output_file = self.docs_structure['standards'] / 'pyspark_coding_standards.json'
        with open(output_file, 'w') as f:
            json.dump(standards, f, indent=2)
        
        logger.info(f"Coding standards generated: {output_file}")
        return standards
    
    def generate_transformation_mapping_guide(self) -> Dict[str, Any]:
        """Generate PowerCenter to PySpark transformation mapping guide"""
        
        mapping_guide = {
            "title": "PowerCenter to PySpark Transformation Mapping Guide",
            "version": "1.0.0",
            "transformations": {
                "source_qualifier": {
                    "powercenter": "Source Qualifier Transformation",
                    "pyspark_equivalent": "spark.read with filter",
                    "description": "Read from source and apply basic filtering",
                    "mapping": {
                        "sql_override": "Use SQL in spark.read.jdbc or custom query",
                        "source_filter": "Use DataFrame.filter() method",
                        "distinct": "Use DataFrame.distinct() or dropDuplicates()"
                    },
                    "example": '''
# PowerCenter: Source Qualifier with SQL Override
# SELECT * FROM customers WHERE status = 'Active'

# PySpark Equivalent
customer_df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT * FROM customers WHERE status = 'Active') as sq",
    properties=connection_props
)

# Or using DataFrame API
customer_df = spark.read.jdbc(url=jdbc_url, table="customers", properties=props) \\
    .filter(col("status") == "Active")
'''
                },
                "expression": {
                    "powercenter": "Expression Transformation",
                    "pyspark_equivalent": "withColumn() and select()",
                    "description": "Derive new columns using expressions",
                    "mapping": {
                        "variable_ports": "Define intermediate columns using withColumn()",
                        "output_ports": "Final select() with required columns",
                        "functions": "Use pyspark.sql.functions"
                    },
                    "example": '''
# PowerCenter: Expression with multiple ports
# Variable: v_full_name = first_name || ' ' || last_name
# Output: o_full_name = v_full_name

# PySpark Equivalent
from pyspark.sql.functions import concat_ws, upper, when

result_df = customer_df \\
    .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) \\
    .withColumn("full_name_upper", upper(col("full_name"))) \\
    .withColumn("name_length", length(col("full_name"))) \\
    .withColumn("is_vip", when(col("total_spend") > 10000, "Y").otherwise("N"))
'''
                },
                "filter": {
                    "powercenter": "Filter Transformation",
                    "pyspark_equivalent": "filter() or where()",
                    "description": "Filter rows based on conditions",
                    "mapping": {
                        "filter_condition": "Boolean expression in filter()",
                        "multiple_conditions": "Combine with & (and) or | (or)"
                    },
                    "example": '''
# PowerCenter: Filter Transformation
# Filter Condition: status = 'Active' AND total_amount > 1000

# PySpark Equivalent
active_customers_df = customer_df \\
    .filter((col("status") == "Active") & (col("total_amount") > 1000))

# Alternative syntax
active_customers_df = customer_df \\
    .where("status = 'Active' AND total_amount > 1000")
'''
                },
                "aggregator": {
                    "powercenter": "Aggregator Transformation",
                    "pyspark_equivalent": "groupBy() with agg()",
                    "description": "Perform aggregations on grouped data",
                    "mapping": {
                        "group_by_ports": "groupBy() columns",
                        "aggregate_expressions": "agg() with aggregate functions",
                        "sorted_input": "Use orderBy() if needed"
                    },
                    "example": '''
# PowerCenter: Aggregator
# Group By: customer_id, region
# Aggregates: SUM(amount), COUNT(*), MAX(transaction_date)

# PySpark Equivalent
from pyspark.sql.functions import sum, count, max, avg

aggregated_df = transaction_df \\
    .groupBy("customer_id", "region") \\
    .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count"),
        max("transaction_date").alias("last_transaction_date"),
        avg("amount").alias("avg_amount")
    )
'''
                },
                "joiner": {
                    "powercenter": "Joiner Transformation",
                    "pyspark_equivalent": "join()",
                    "description": "Join two sources based on join condition",
                    "mapping": {
                        "master_source": "Right side of join (broadcast if small)",
                        "detail_source": "Left side of join",
                        "join_condition": "Join key(s)",
                        "join_type": "inner, left, right, full, left_anti, left_semi"
                    },
                    "example": '''
# PowerCenter: Joiner (Normal Join, Master: customers, Detail: orders)
# Join Condition: customers.customer_id = orders.customer_id

# PySpark Equivalent - Regular Join
result_df = orders_df.join(customers_df, "customer_id", "inner")

# PySpark - Broadcast Join (for small dimension tables)
from pyspark.sql.functions import broadcast

result_df = orders_df.join(broadcast(customers_df), "customer_id", "left")

# Complex join condition
result_df = orders_df.alias("o").join(
    customers_df.alias("c"),
    (col("o.customer_id") == col("c.customer_id")) & 
    (col("o.order_date") >= col("c.start_date")),
    "inner"
)
'''
                },
                "lookup": {
                    "powercenter": "Lookup Transformation",
                    "pyspark_equivalent": "join() with left join",
                    "description": "Lookup reference data",
                    "mapping": {
                        "connected_lookup": "left join with broadcast",
                        "unconnected_lookup": "UDF with broadcast variable",
                        "cache": "Use broadcast() for small lookups"
                    },
                    "example": '''
# PowerCenter: Connected Lookup
# Lookup Table: product_master
# Lookup Condition: product_id
# Return: product_name, category

# PySpark Equivalent
lookup_df = spark.read.table("product_master") \\
    .select("product_id", "product_name", "category")

result_df = transaction_df.join(
    broadcast(lookup_df),
    "product_id",
    "left"
)

# Handle multiple return values on match
result_df = result_df \\
    .withColumn("product_name", coalesce(col("product_name"), lit("Unknown"))) \\
    .withColumn("category", coalesce(col("category"), lit("Uncategorized")))
'''
                },
                "router": {
                    "powercenter": "Router Transformation",
                    "pyspark_equivalent": "Multiple filter() operations",
                    "description": "Route rows to different targets based on conditions",
                    "mapping": {
                        "router_groups": "Separate DataFrames with different filters",
                        "default_group": "Final DataFrame with negated conditions"
                    },
                    "example": '''
# PowerCenter: Router with 3 groups
# Group1: amount > 10000 (high_value)
# Group2