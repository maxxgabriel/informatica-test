"""
Informatica PowerCenter to PySpark Migration Runbook Generator
===============================================================

This module generates comprehensive migration documentation, runbooks, and 
knowledge base materials for Informatica to PySpark migrations.

Author: Data Engineering Team
Version: 1.0.0
License: Enterprise
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
import json
import os
from typing import Dict, List, Any
import yaml


class MigrationRunbookGenerator:
    """
    Generates comprehensive migration documentation and runbooks for 
    Informatica PowerCenter to PySpark migration projects.
    """
    
    def __init__(self, output_base_path: str):
        """
        Initialize the runbook generator.
        
        Args:
            output_base_path: Base directory for documentation output
        """
        self.output_base_path = output_base_path
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create documentation structure
        self.doc_structure = {
            'migration_runbook': f"{output_base_path}/migration_runbook",
            'coding_standards': f"{output_base_path}/coding_standards",
            'transformation_mapping': f"{output_base_path}/transformation_mapping",
            'operational_runbooks': f"{output_base_path}/operational_runbooks",
            'troubleshooting': f"{output_base_path}/troubleshooting",
            'monitoring': f"{output_base_path}/monitoring",
            'training': f"{output_base_path}/training",
            'knowledge_transfer': f"{output_base_path}/knowledge_transfer"
        }
        
        self._create_directory_structure()
    
    def _create_directory_structure(self):
        """Create the documentation directory structure."""
        for path in self.doc_structure.values():
            os.makedirs(path, exist_ok=True)
    
    def generate_migration_runbook(self) -> str:
        """Generate comprehensive migration runbook."""
        
        runbook_content = {
            "title": "Informatica PowerCenter to PySpark Migration Runbook",
            "version": "1.0.0",
            "last_updated": datetime.now().isoformat(),
            
            "overview": {
                "purpose": "Guide for migrating Informatica PowerCenter workflows to PySpark",
                "scope": "End-to-end migration including assessment, conversion, testing, and deployment",
                "audience": ["Data Engineers", "DevOps Engineers", "QA Analysts", "Business Analysts"]
            },
            
            "migration_phases": {
                "phase_1_assessment": {
                    "name": "Discovery and Assessment",
                    "duration": "2-3 weeks",
                    "activities": [
                        {
                            "task": "Inventory Existing Workflows",
                            "description": "Catalog all PowerCenter workflows, sessions, and mappings",
                            "deliverable": "Complete inventory spreadsheet with complexity ratings",
                            "tools": ["PowerCenter Repository Browser", "Metadata extraction scripts"],
                            "steps": [
                                "Connect to PowerCenter repository",
                                "Export folder structure and object counts",
                                "Document dependencies and lineage",
                                "Identify reusable components",
                                "Calculate complexity scores"
                            ]
                        },
                        {
                            "task": "Analyze Source to Target Mappings",
                            "description": "Document all data transformations and business logic",
                            "deliverable": "Transformation specification document",
                            "steps": [
                                "Extract mapping logic from PowerCenter",
                                "Document transformation rules",
                                "Identify complex transformations requiring custom code",
                                "Map PowerCenter transformations to PySpark equivalents"
                            ]
                        },
                        {
                            "task": "Assess Data Volumes and Performance",
                            "description": "Analyze current performance metrics and SLAs",
                            "deliverable": "Performance baseline document",
                            "steps": [
                                "Collect workflow execution statistics",
                                "Document data volumes and growth trends",
                                "Identify performance bottlenecks",
                                "Define target performance SLAs"
                            ]
                        }
                    ]
                },
                
                "phase_2_design": {
                    "name": "Solution Design",
                    "duration": "2-4 weeks",
                    "activities": [
                        {
                            "task": "Design PySpark Architecture",
                            "description": "Define target state architecture and patterns",
                            "deliverable": "Architecture design document",
                            "components": [
                                "Data lake structure (Bronze/Silver/Gold layers)",
                                "Spark cluster configuration",
                                "Job orchestration framework",
                                "Error handling and logging strategy",
                                "Metadata management approach"
                            ]
                        },
                        {
                            "task": "Create Transformation Framework",
                            "description": "Build reusable PySpark transformation library",
                            "deliverable": "Framework code repository",
                            "components": [
                                "Common transformation functions",
                                "Data quality validation framework",
                                "Configuration management utilities",
                                "Logging and monitoring framework"
                            ]
                        }
                    ]
                },
                
                "phase_3_development": {
                    "name": "Code Development and Conversion",
                    "duration": "8-12 weeks",
                    "activities": [
                        {
                            "task": "Convert PowerCenter Mappings to PySpark",
                            "description": "Systematic conversion of all workflows",
                            "approach": "Prioritize by business criticality and complexity",
                            "steps": [
                                "Select workflow batch for conversion",
                                "Extract PowerCenter mapping XML",
                                "Generate PySpark code using templates",
                                "Implement business logic transformations",
                                "Add error handling and logging",
                                "Create unit tests",
                                "Peer code review",
                                "Commit to version control"
                            ],
                            "quality_gates": [
                                "Code adheres to standards",
                                "Unit test coverage > 80%",
                                "Peer review completed",
                                "Static code analysis passed"
                            ]
                        },
                        {
                            "task": "Implement Job Orchestration",
                            "description": "Configure workflow scheduling and dependencies",
                            "tools": ["Apache Airflow", "Azure Data Factory", "AWS Step Functions"],
                            "steps": [
                                "Define DAGs/Pipelines",
                                "Configure job dependencies",
                                "Set up retry and alerting logic",
                                "Implement parameter passing",
                                "Configure scheduling"
                            ]
                        }
                    ]
                },
                
                "phase_4_testing": {
                    "name": "Testing and Validation",
                    "duration": "4-6 weeks",
                    "activities": [
                        {
                            "task": "Unit Testing",
                            "coverage": "Individual transformation functions",
                            "framework": "pytest",
                            "success_criteria": ">80% code coverage"
                        },
                        {
                            "task": "Integration Testing",
                            "coverage": "End-to-end data flows",
                            "approach": "Compare PowerCenter vs PySpark outputs",
                            "success_criteria": "100% data reconciliation"
                        },
                        {
                            "task": "Performance Testing",
                            "coverage": "Load and stress testing",
                            "success_criteria": "Meet or exceed baseline SLAs"
                        },
                        {
                            "task": "UAT",
                            "coverage": "Business validation",
                            "participants": "Business stakeholders",
                            "success_criteria": "Sign-off on all test cases"
                        }
                    ]
                },
                
                "phase_5_deployment": {
                    "name": "Production Deployment",
                    "duration": "2-3 weeks",
                    "activities": [
                        {
                            "task": "Pre-Deployment Validation",
                            "checklist": [
                                "All tests passed in UAT",
                                "Performance benchmarks met",
                                "Runbooks completed and reviewed",
                                "Support team trained",
                                "Rollback plan documented",
                                "Change management approval obtained"
                            ]
                        },
                        {
                            "task": "Deployment Execution",
                            "approach": "Phased rollout by workflow groups",
                            "steps": [
                                "Deploy to production environment",
                                "Run parallel PowerCenter and PySpark",
                                "Validate outputs match",
                                "Monitor performance metrics",
                                "Cutover from PowerCenter to PySpark",
                                "Decommission PowerCenter workflows"
                            ]
                        },
                        {
                            "task": "Post-Deployment Validation",
                            "duration": "1-2 weeks",
                            "activities": [
                                "Monitor production runs",
                                "Validate data quality",
                                "Track performance metrics",
                                "Address any issues",
                                "Document lessons learned"
                            ]
                        }
                    ]
                },
                
                "phase_6_hypercare": {
                    "name": "Hypercare and Support",
                    "duration": "4 weeks post-deployment",
                    "activities": [
                        {
                            "task": "Intensive Monitoring",
                            "description": "24/7 monitoring and rapid response",
                            "sla": "< 15 minutes response time for critical issues"
                        },
                        {
                            "task": "Issue Resolution",
                            "description": "Quick triage and fix of production issues",
                            "escalation_matrix": "Defined support tiers"
                        },
                        {
                            "task": "Performance Tuning",
                            "description": "Optimize based on production patterns",
                            "focus_areas": ["Query optimization", "Partition tuning", "Resource allocation"]
                        }
                    ]
                }
            },
            
            "roles_responsibilities": {
                "migration_lead": {
                    "responsibilities": [
                        "Overall migration program management",
                        "Stakeholder communication",
                        "Risk management",
                        "Resource allocation",
                        "Timeline management"
                    ]
                },
                "solution_architect": {
                    "responsibilities": [
                        "Design target state architecture",
                        "Define technical standards",
                        "Review complex transformations",
                        "Technology selection",
                        "Performance optimization strategy"
                    ]
                },
                "lead_developer": {
                    "responsibilities": [
                        "Framework development",
                        "Code review and quality assurance",
                        "Developer mentoring",
                        "Technical documentation",
                        "Complex transformation implementation"
                    ]
                },
                "developers": {
                    "responsibilities": [
                        "PowerCenter analysis",
                        "PySpark code development",
                        "Unit testing",
                        "Documentation",
                        "Code reviews"
                    ]
                },
                "qa_analyst": {
                    "responsibilities": [
                        "Test strategy and planning",
                        "Test case development",
                        "Test execution",
                        "Data reconciliation",
                        "Defect management"
                    ]
                },
                "devops_engineer": {
                    "responsibilities": [
                        "Environment setup",
                        "CI/CD pipeline development",
                        "Deployment automation",
                        "Infrastructure management",
                        "Monitoring setup"
                    ]
                }
            },
            
            "risk_mitigation": {
                "common_risks": [
                    {
                        "risk": "Data quality issues discovered post-migration",
                        "impact": "High",
                        "probability": "Medium",
                        "mitigation": [
                            "Comprehensive data profiling in assessment phase",
                            "Automated data reconciliation testing",
                            "Implement data quality checks in PySpark jobs",
                            "Run parallel processing during transition"
                        ]
                    },
                    {
                        "risk": "Performance degradation vs PowerCenter",
                        "impact": "High",
                        "probability": "Medium",
                        "mitigation": [
                            "Early performance testing",
                            "Proper partitioning and caching strategy",
                            "Right-sizing Spark clusters",
                            "Query optimization and tuning",
                            "Performance baseline documentation"
                        ]
                    },
                    {
                        "risk": "Complex transformation logic difficult to replicate",
                        "impact": "Medium",
                        "probability": "High",
                        "mitigation": [
                            "Detailed mapping documentation",
                            "Engage business SMEs for validation",
                            "Prototype complex transformations early",
                            "Create reusable transformation library"
                        ]
                    },
                    {
                        "risk": "Resource availability and skill gaps",
                        "impact": "High",
                        "probability": "Medium",
                        "mitigation": [
                            "Early team training on PySpark",
                            "Hire experienced Spark developers",
                            "Develop comprehensive documentation",
                            "Implement pair programming",
                            "Create knowledge sharing sessions"
                        ]
                    }
                ]
            }
        }
        
        output_file = f"{self.doc_structure['migration_runbook']}/migration_runbook_{self.timestamp}.yaml"
        with open(output_file, 'w') as f:
            yaml.dump(runbook_content, f, default_flow_style=False, sort_keys=False)
        
        return output_file
    
    def generate_coding_standards(self) -> str:
        """Generate PySpark coding standards and best practices."""
        
        standards_content = {
            "title": "PySpark Coding Standards and Best Practices",
            "version": "1.0.0",
            "last_updated": datetime.now().isoformat(),
            
            "general_principles": {
                "readability": "Code should be self-documenting and easy to understand",
                "maintainability": "Write modular, reusable code with clear separation of concerns",
                "performance": "Optimize for distributed processing and minimize data shuffling",
                "reliability": "Implement comprehensive error handling and logging",
                "testability": "Design code to be easily unit testable"
            },
            
            "naming_conventions": {
                "variables": {
                    "style": "snake_case",
                    "examples": {
                        "good": ["customer_df", "total_amount", "is_active"],
                        "bad": ["customerDf", "TotalAmount", "isActive"]
                    },
                    "rules": [
                        "Use descriptive names that indicate purpose",
                        "Avoid single letter names except for iterators",
                        "Boolean variables should start with is_, has_, can_",
                        "DataFrame variables should end with _df"
                    ]
                },
                "functions": {
                    "style": "snake_case",
                    "examples": {
                        "good": ["transform_customer_data", "calculate_total", "validate_input"],
                        "bad": ["TransformCustomerData", "calculateTotal", "ValidateInput"]
                    },
                    "rules": [
                        "Use verb phrases that describe action",
                        "Keep names concise but descriptive",
                        "Avoid abbreviations unless widely understood"
                    ]
                },
                "classes": {
                    "style": "PascalCase",
                    "examples": {
                        "good": ["CustomerTransformer", "DataValidator", "ConfigManager"],
                        "bad": ["customer_transformer", "datavalidator", "config_Manager"]
                    }
                },
                "constants": {
                    "style": "UPPER_SNAKE_CASE",
                    "examples": {
                        "good": ["MAX_RETRY_COUNT", "DEFAULT_PARTITION_SIZE", "CONFIG_FILE_PATH"],
                        "bad": ["max_retry_count", "DefaultPartitionSize", "configFilePath"]
                    }
                }
            },
            
            "code_structure": {
                "file_organization": {
                    "header": [
                        "Module docstring with description",
                        "Author and version information",
                        "Import statements (standard library, third-party, local)"
                    ],
                    "sections": [
                        "Constants and configuration",
                        "Helper functions",
                        "Main transformation functions",
                        "Entry point (if applicable)"
                    ],
                    "example": '''"""
Customer Data Transformation Module

This module processes customer data from raw to curated layer,
applying business rules and data quality validations.

Author: Data Engineering Team
Version: 1.0.0
"""

# Standard library imports
from datetime import datetime
import logging

# Third-party imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType

# Local imports
from common.utils import get_spark_session, log_dataframe_info
from common.validators import validate_schema

# Constants
DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
MAX_STRING_LENGTH = 255

# Configuration
logger = logging.getLogger(__name__)
'''
                },
                
                "function_structure": {
                    "components": [
                        "Function signature with type hints",
                        "Comprehensive docstring",
                        "Input validation",
                        "Main logic",
                        "Return statement"
                    ],
                    "example": '''def transform_customer_data(
    input_df: DataFrame,
    business_date: str,
    config: Dict[str, Any]
) -> DataFrame:
    """
    Transform customer data by applying business rules and enrichments.
    
    This function performs the following transformations:
    1. Standardizes customer names
    2. Validates email formats
    3. Calculates customer age
    4. Applies business status rules
    
    Args:
        input_df: Input DataFrame containing raw customer data
        business_date: Processing date in YYYY-MM-DD format
        config: Configuration dictionary with transformation parameters
    
    Returns:
        Transformed DataFrame with standardized customer data
    
    Raises:
        ValueError: If business_date format is invalid
        TypeError: If input_df is not a DataFrame
    
    Example:
        >>> config = {"default_country": "US", "min_age": 18}
        >>> result_df = transform_customer_data(raw_df, "2024-01-01", config)
    """
    # Input validation
    if not isinstance(input_df, DataFrame):
        raise TypeError("input_df must be a PySpark DataFrame")
    
    # Main transformation logic
    transformed_df = (
        input_df
        .withColumn("customer_name", upper(trim(col("customer_name"))))
        .withColumn("is_valid_email", col("email").rlike(r"^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"))
        .withColumn("processing_date", lit(business_date))
    )
    
    return transformed_df
'''
                }
            },
            
            "pyspark_best_practices": {
                "dataframe_operations": {
                    "practices": [
                        {
                            "practice": "Use DataFrame API over RDD API",
                            "reason": "Better optimization by Catalyst optimizer",
                            "good_example": "df.filter(col('age') > 18).select('name', 'age')",
                            "bad_example": "df.rdd.filter(lambda x: x['age'] > 18).map(lambda x: (x['name'], x['age'])).toDF()"
                        },
                        {
                            "practice": "Chain transformations efficiently",
                            "reason": "Improves readability and allows for optimization",
                            "good_example": '''result_df = (
    input_df
    .filter(col("status") == "active")
    .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
    .select("customer_id", "full_name", "email")
)''',
                            "bad_example": '''df1 = input_df.filter(col("status") == "active")
df2 = df1.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
result_df = df2.select("customer_id", "full_name", "email")'''
                        },
                        {
                            "practice": "Avoid unnecessary actions",
                            "reason": "Actions trigger job execution; minimize for performance",
                            "good_example": "# Only call .count() or .show() when needed for debugging",
                            "bad_example": "df.count()  # Called multiple times unnecessarily"
                        },
                        {
                            "practice": "Use appropriate join types",
                            "reason": "Choose the right join strategy for performance",
                            "good_example": "large_df.join(broadcast(small_df), 'key', 'left')",
                            "bad_example": "large_df.join(large_df2, 'key')  # No broadcast hint for small table"
                        }
                    ]
                },
                
                "performance_optimization": {
                    "techniques": [
                        {
                            "technique": "Partitioning",
                            "description": "Distribute data across cluster nodes efficiently",
                            "example": '''# Repartition for better parallelism
df = df.repartition(100, "customer_id")

# Coalesce to reduce partitions after filtering
filtered_df = df.filter(col("status") == "active").coalesce(10)'''
                        },
                        {
                            "technique": "Caching",
                            "description": "Cache DataFrames that are reused multiple times",
                            "example": '''# Cache DataFrame used multiple times
base_df = input_df.filter(col("date") >= "2024-01-01").cache()

result1 = base_df.groupBy("region").count()
result2 = base_df.groupBy("product").sum("amount")

# Unpersist when no longer needed
base_df.unpersist()'''
                        },
                        {
                            "technique": "Broadcast Joins",
                            "description": "Use broadcast for small dimension tables",
                            "example": '''from pyspark.sql.functions import broadcast

# Broadcast small lookup table
result = fact_df.join(
    broadcast(dimension_df),
    "dimension_key",
    "left"
)'''
                        },
                        {
                            "technique": "Predicate Pushdown",
                            "description": "Filter data as early as possible",
                            "example": '''# Good: Filter before join
filtered_df = input_df.filter(col("status") == "active")
result = filtered_df.join(other_df, "key")

# Bad: Filter after join
joined_df = input_df.join(other_df, "key")
result = joined_df.filter(col("status") == "active")'''
                        },
                        {
                            "technique": "Column Pruning",
                            "description": "Select only required columns early",
                            "example": '''# Good: Select columns early
selected_df = input_df.select("id", "name", "amount")
result = selected_df.groupBy("name").sum("amount")

# Bad: Carry all columns through transformations
result = input_df.groupBy("name").sum("amount")'''
                        }
                    ]
                },
                
                "data_quality": {
                    "practices": [
                        {
                            "practice": "Schema Validation",
                            "description": "Validate input data schema",
                            "example": '''def validate_schema(df: DataFrame, expected_schema: StructType) -> bool:
    """Validate that DataFrame matches expected schema."""
    return df.schema == expected_schema

# Usage
if not validate_schema(input_df, expected_customer_schema):
    raise ValueError("Input schema does not match expected schema")'''
                        },
                        {
                            "practice": "Null Handling",
                            "description": "Explicitly handle null values",
                            "example": '''# Handle nulls appropriately
result_df = (
    input_df
    .withColumn("email", coalesce(col("email"), lit("unknown@example.com")))
    .withColumn("age", when(col("age").isNull(), lit(0)).otherwise(col("age")))
)'''
                        },
                        {
                            "practice": "Data Validation",
                            "description": "Add data quality checks",
                            "example": '''# Add validation columns
validated_df = (
    input_df
    .withColumn("is_valid_email", col("email").rlike(r"^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"))
    .withColumn("is_valid_age", col("age").between(0, 120))
)

# Filter or flag invalid records
valid_df = validated_df.filter(col("is_valid_email") & col("is_valid_age"))
invalid_df = validated_df.filter(~(col("is_valid_email") & col("is_valid_age")))'''
                        }
                    ]
                },
                
                "error_handling": {
                    "practices": [
                        {
                            "practice": "Try-Except Blocks",
                            "description": "Handle exceptions appropriately",
                            "example": '''def safe_transform(df: DataFrame) -> DataFrame:
    """Apply transformation with error handling."""
    try:
        result_df = df.transform(complex_transformation)
        logger.info(f"Transformation completed successfully. Record count: {result_df.count()}")
        return result_df
    except AnalysisException as e:
        logger.error(f"Analysis error during transformation: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during transformation: {str(e)}")
        raise'''
                        },
                        {
                            "practice": "Data Validation Errors",
                            "description": "Validate and handle bad data",
                            "example": '''def process_with_error_handling(df: DataFrame, error_path: str) -> DataFrame:
    """Process data and write errors to separate location."""
    
    # Add validation columns
    validated_df = df.withColumn(
        "has_errors",
        when(col("customer_id").isNull(), True)
        .when(col("amount") < 0, True)
        .otherwise(False)
    )
    
    # Separate valid and invalid records
    valid_df = validated_df.filter(~col("has_errors")).drop("has_errors")
    error_df = validated_df.filter(col("has_errors"))
    
    # Write errors to error path
    if error_df.count() > 0:
        error_df.write.mode("append").parquet(error_path)
        logger.warning(f"Found {error_df.count()} error records")
    
    return valid_df'''
                        }
                    ]
                },
                
                "logging": {
                    "practices": [
                        {
                            "practice": "Use Python Logging Module",
                            "description": "Structured logging with appropriate levels",
                            "example": '''import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Log at appropriate levels
def process_data(df: DataFrame) -> DataFrame:
    logger.info("Starting data processing")
    logger.debug(f"Input schema: {df.schema}")
    
    try:
        result_df = df.transform(apply_transformations)
        logger.info(f"Processing completed. Output records: {result_df.count()}")
        return result_df
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}", exc_info=True)
        raise'''
                        },
                        {
                            "practice": "Log Key Metrics",
                            "description": "Track processing metrics",
                            "example": '''def log_dataframe_metrics(df: DataFrame, stage: str):
    """Log key DataFrame metrics."""
    record_count = df.count()
    partition_count = df.rdd.getNumPartitions()
    
    logger.info(f"{stage} - Record count: {record_count}")
    logger.info(f"{stage} - Partition count: {partition_count}")
    logger.info(f"{stage} - Schema: {df.schema.simpleString()}")'''
                        }
                    ]
                }
            },
            
            "documentation_standards": {
                "docstrings": {
                    "format": "Google Style or NumPy Style",
                    "required_sections": [
                        "Brief description",
                        "Detailed description (if needed)",
                        "Args section with parameter descriptions",
                        "Returns section with return value description",
                        "Raises section with exception descriptions",
                        "Example section with usage examples"
                    ],
                    "example": '''def aggregate_customer_metrics(
    transactions_df: DataFrame,
    start_date: str,
    end_date: str,
    metric_types: List[str]
) -> DataFrame:
    """
    Aggregate customer transaction metrics for a date range.
    
    This function calculates various customer metrics including transaction
    count, total amount, and average transaction value. Metrics are filtered
    by the specified date range and metric types.
    
    Args:
        transactions_df: DataFrame containing transaction records with columns:
            - customer_id (string): Unique customer identifier
            - transaction_date (date): Date of transaction
            - amount (decimal): Transaction amount
            - transaction_type (string): Type of transaction
        start_date: Start date for metric calculation (format: YYYY-MM-DD)
        end_date: End date for metric calculation (format: YYYY-MM-DD)
        metric_types: List of metric types to calculate. Valid values:
            ["count", "sum", "avg", "min", "max"]
    
    Returns:
        DataFrame with aggregated metrics containing columns:
        - customer_id: Unique customer identifier
        - metric_type: Type of metric calculated
        - metric_value: Calculated metric value
        - calculation_date: Date of calculation
    
    Raises:
        ValueError: If start_date is after end_date
        ValueError: If metric_types contains invalid values
        TypeError: If transactions_df is not a DataFrame
    
    Example:
        >>> transactions = spark.read.parquet("/data/transactions")
        >>> metrics = aggregate_customer_metrics(
        ...     transactions,
        ...     "2024-01-01",
        ...     "2024-12-31",
        ...     ["count", "sum", "avg"]
        ... )
        >>> metrics.show()
        +-----------+-----------+------------+-----------------+
        |customer_id|metric_type|metric_value|calculation_date|
        +-----------+-----------+------------+-----------------+
        |CUST001    |count      |45.0        |2024-01-01      |
        |CUST001    |sum        |12500.75    |2024-01-01      |
        +-----------+-----------+------------+-----------------+
    
    Note:
        This function caches the input DataFrame if it will be reused
        for multiple metric calculations. Ensure to unpersist when done.
    """
    # Implementation here
    pass'''
                },
                
                "inline_comments": {
                    "guidelines": [
                        "Explain WHY, not WHAT (code should be self-explanatory)",
                        "Keep comments concise and up-to-date",
                        "Use comments for complex business logic",
                        "Document assumptions and limitations"
                    ],
                    "examples": {
                        "good": [
                            "# Use broadcast join because lookup table < 100MB",
                            "# Apply business rule: exclude cancelled orders from last 7 days",
                            "# Coalesce to 10 partitions to avoid small files"
                        ],
                        "bad": [
                            "# Loop through rows",
                            "# Add column",
                            "# Filter DataFrame"
                        ]
                    }
                }
            },
            
            "testing_standards": {
                "unit_tests": {
                    "framework": "pytest",
                    "coverage_target": "80%",
                    "structure": '''import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType