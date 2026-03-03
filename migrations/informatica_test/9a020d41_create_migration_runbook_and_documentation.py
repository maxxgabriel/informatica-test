"""
Informatica to PySpark Migration - Comprehensive Documentation Generator
========================================================================

This module generates all necessary documentation for migrating from Informatica
PowerCenter to PySpark, including runbooks, standards, and training materials.

Author: Data Engineering Team
Version: 1.0.0
Last Updated: 2024
"""

from pyspark.sql import SparkSession
from datetime import datetime
import json
import os
from typing import Dict, List, Any
from dataclasses import dataclass, asdict
from pathlib import Path


# ============================================================================
# CONFIGURATION AND DATA CLASSES
# ============================================================================

@dataclass
class MigrationMetadata:
    """Metadata for migration documentation"""
    project_name: str
    version: str
    created_date: str
    last_updated: str
    team_members: List[str]
    environments: List[str]


@dataclass
class TransformationMapping:
    """Mapping between Informatica and PySpark transformations"""
    informatica_component: str
    pyspark_equivalent: str
    complexity: str
    example_code: str
    notes: str


@dataclass
class CodingStandard:
    """PySpark coding standard definition"""
    category: str
    standard: str
    rationale: str
    example_good: str
    example_bad: str


@dataclass
class OperationalProcedure:
    """Operational procedure definition"""
    procedure_name: str
    category: str
    steps: List[str]
    prerequisites: List[str]
    expected_outcome: str
    rollback_steps: List[str]


@dataclass
class TroubleshootingGuide:
    """Troubleshooting guide entry"""
    issue_category: str
    symptom: str
    possible_causes: List[str]
    resolution_steps: List[str]
    prevention: str


@dataclass
class MonitoringAlert:
    """Monitoring and alerting definition"""
    alert_name: str
    severity: str
    metric: str
    threshold: str
    action: str
    escalation_path: List[str]


# ============================================================================
# MIGRATION RUNBOOK GENERATOR
# ============================================================================

class MigrationRunbookGenerator:
    """Generates comprehensive migration runbook"""
    
    def __init__(self, output_path: str):
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        
    def generate_runbook(self) -> str:
        """Generate complete migration runbook"""
        runbook = """
# INFORMATICA TO PYSPARK MIGRATION RUNBOOK
==========================================

## Table of Contents
1. Migration Overview
2. Pre-Migration Phase
3. Migration Execution Phase
4. Post-Migration Phase
5. Rollback Procedures
6. Success Criteria

---

## 1. MIGRATION OVERVIEW

### 1.1 Purpose
This runbook provides step-by-step procedures for migrating Informatica PowerCenter 
workflows to PySpark on cloud/on-premise infrastructure.

### 1.2 Scope
- ETL workflows and mappings
- Data transformations
- Job scheduling
- Monitoring and alerting
- Data quality rules

### 1.3 Migration Approach
- **Phase 1**: Assessment and Planning
- **Phase 2**: Development and Testing
- **Phase 3**: User Acceptance Testing
- **Phase 4**: Production Deployment
- **Phase 5**: Hypercare and Optimization

---

## 2. PRE-MIGRATION PHASE

### 2.1 Environment Setup

#### 2.1.1 Development Environment
```bash
# Install required tools
pip install pyspark==3.4.0
pip install pytest==7.4.0
pip install great-expectations==0.17.0
pip install delta-spark==2.4.0

# Configure Spark environment
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=python3.9
export PYSPARK_DRIVER_PYTHON=python3.9
```

#### 2.1.2 Cloud Infrastructure (if applicable)
- Provision Databricks/EMR cluster
- Configure S3/ADLS storage
- Set up networking and security groups
- Configure IAM roles and permissions

#### 2.1.3 Version Control Setup
```bash
# Initialize repository
git clone https://github.com/org/pyspark-migration.git
cd pyspark-migration
git checkout -b feature/migration-batch-1

# Set up directory structure
mkdir -p {src,tests,config,docs,scripts}
```

### 2.2 Assessment Activities

#### 2.2.1 Informatica Inventory
- Export all workflow XMLs
- Document source-target mappings
- Identify complex transformations
- List all dependencies
- Document scheduling requirements

#### 2.2.2 Complexity Analysis
| Component Type | Count | Complexity | Priority |
|---------------|-------|-----------|----------|
| Mappings      | XXX   | High/Med  | P1       |
| Workflows     | XXX   | Medium    | P1       |
| Worklets      | XXX   | Low       | P2       |
| Sessions      | XXX   | Low       | P2       |

#### 2.2.3 Risk Assessment
- Data quality risks
- Performance risks
- Integration risks
- Timeline risks

### 2.3 Planning Activities

#### 2.3.1 Migration Waves
```
Wave 1: Simple mappings (No transformation complexity)
Wave 2: Medium complexity (Standard transformations)
Wave 3: Complex mappings (Custom logic, lookups)
Wave 4: Critical workflows (High business impact)
```

#### 2.3.2 Resource Allocation
- Developers: X FTE
- Testers: X FTE
- Infrastructure: X hours
- Timeline: X weeks

---

## 3. MIGRATION EXECUTION PHASE

### 3.1 Code Development

#### 3.1.1 Transformation Development Steps
1. Review Informatica mapping XML
2. Create PySpark transformation skeleton
3. Implement business logic
4. Add data quality checks
5. Implement error handling
6. Add logging and monitoring
7. Write unit tests
8. Conduct code review
9. Update documentation

#### 3.1.2 Development Checklist
- [ ] Source systems connectivity verified
- [ ] Target systems connectivity verified
- [ ] Configuration externalized
- [ ] Error handling implemented
- [ ] Logging framework integrated
- [ ] Data quality checks added
- [ ] Unit tests written (>80% coverage)
- [ ] Integration tests created
- [ ] Documentation updated
- [ ] Code review completed

### 3.2 Testing Phase

#### 3.2.1 Unit Testing
```bash
# Run unit tests
pytest tests/unit/ -v --cov=src --cov-report=html

# Expected output: >80% coverage
```

#### 3.2.2 Integration Testing
```bash
# Run integration tests with test data
pytest tests/integration/ -v --env=dev

# Validate against Informatica output
python scripts/data_validation.py --source informatica --target pyspark
```

#### 3.2.3 Performance Testing
```bash
# Run performance benchmarks
python scripts/performance_test.py --data-volume large

# Expected SLA: <Informatica runtime + 10%
```

#### 3.2.4 Data Validation Steps
1. Row count validation
2. Column schema validation
3. Data type validation
4. Null value validation
5. Business rule validation
6. Aggregate validation
7. Sample data comparison

### 3.3 User Acceptance Testing

#### 3.3.1 UAT Preparation
- Prepare UAT test cases
- Set up UAT environment
- Load test data
- Configure access for business users

#### 3.3.2 UAT Execution
- Business users execute test scenarios
- Document results
- Track defects
- Retest fixes
- Obtain sign-off

---

## 4. POST-MIGRATION PHASE

### 4.1 Production Deployment

#### 4.1.1 Deployment Checklist
- [ ] Code promoted to production repository
- [ ] Configuration files updated for production
- [ ] Database connections verified
- [ ] Service accounts configured
- [ ] Scheduler jobs created
- [ ] Monitoring alerts configured
- [ ] Runbook updated
- [ ] Team trained
- [ ] Backup of Informatica workflows
- [ ] Change management approval

#### 4.1.2 Deployment Steps
```bash
# 1. Take backup of current production
python scripts/backup_production.py --environment prod

# 2. Deploy PySpark code
python scripts/deploy.py --environment prod --version v1.0.0

# 3. Update scheduler
python scripts/update_scheduler.py --config config/prod/scheduler.json

# 4. Smoke test
python scripts/smoke_test.py --environment prod

# 5. Monitor first run
python scripts/monitor_job.py --job-id <job_id> --duration 60
```

#### 4.1.3 Cutover Activities
1. Disable Informatica workflows (DO NOT DELETE)
2. Enable PySpark jobs
3. Monitor first execution
4. Validate output data
5. Confirm downstream systems
6. Update documentation
7. Notify stakeholders

### 4.2 Hypercare Period

#### 4.2.1 Duration
- Standard: 2 weeks
- Complex: 4 weeks

#### 4.2.2 Activities
- Monitor all job executions
- Track and resolve issues immediately
- Daily status reports
- Performance tuning as needed
- User feedback collection

#### 4.2.3 Success Metrics
- Job success rate: >99%
- Performance: Within SLA
- Data quality: 100% accuracy
- Incidents: <3 per week

---

## 5. ROLLBACK PROCEDURES

### 5.1 Rollback Decision Criteria
- Critical data quality issues
- Performance degradation >50%
- Multiple job failures
- Downstream system impacts
- Business user escalation

### 5.2 Rollback Steps

#### 5.2.1 Immediate Rollback (< 1 hour)
```bash
# 1. Disable PySpark jobs
python scripts/disable_jobs.py --environment prod --job-pattern "migrated_*"

# 2. Re-enable Informatica workflows
# Login to Informatica Workflow Manager
# Enable workflows from backup list

# 3. Verify Informatica execution
# Monitor next scheduled run

# 4. Notify stakeholders
python scripts/send_notification.py --type rollback --severity high
```

#### 5.2.2 Data Recovery (if needed)
```bash
# 1. Identify affected tables
python scripts/identify_affected_data.py --start-time "2024-01-01 00:00:00"

# 2. Restore from backup
python scripts/restore_data.py --backup-id <backup_id> --tables <table_list>

# 3. Validate restored data
python scripts/validate_restore.py --tables <table_list>
```

### 5.3 Post-Rollback Analysis
1. Root cause analysis
2. Fix identification
3. Remediation plan
4. Re-migration schedule

---

## 6. SUCCESS CRITERIA

### 6.1 Technical Criteria
- All jobs complete successfully
- Data validation 100% pass
- Performance within SLA
- No critical defects
- Monitoring operational

### 6.2 Business Criteria
- Business user acceptance
- Downstream systems validated
- Reports accurate
- No business disruption

### 6.3 Sign-off Requirements
- [ ] Technical Lead
- [ ] Business Owner
- [ ] Operations Team
- [ ] QA Lead
- [ ] Project Manager

---

## 7. APPENDICES

### Appendix A: Contact Information
- Technical Lead: [Name] [Email] [Phone]
- Project Manager: [Name] [Email] [Phone]
- Business Owner: [Name] [Email] [Phone]
- On-Call Support: [Phone] [Email]

### Appendix B: Key Documents
- Transformation Mapping Guide
- Coding Standards
- Operational Procedures
- Troubleshooting Guide
- Training Materials

### Appendix C: Change Log
| Date | Version | Author | Changes |
|------|---------|--------|---------|
| 2024-01-01 | 1.0.0 | Team | Initial version |

"""
        
        output_file = self.output_path / "migration_runbook.md"
        with open(output_file, 'w') as f:
            f.write(runbook)
        
        return str(output_file)


# ============================================================================
# CODING STANDARDS GENERATOR
# ============================================================================

class CodingStandardsGenerator:
    """Generates PySpark coding standards documentation"""
    
    def __init__(self, output_path: str):
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        
    def generate_standards(self) -> str:
        """Generate coding standards document"""
        standards = """
# PYSPARK CODING STANDARDS AND BEST PRACTICES
=============================================

## 1. CODE ORGANIZATION

### 1.1 Project Structure
```
project_root/
├── src/
│   ├── jobs/           # Main job scripts
│   ├── transformations/# Transformation logic
│   ├── utils/          # Utility functions
│   ├── config/         # Configuration modules
│   └── common/         # Common/shared code
├── tests/
│   ├── unit/          # Unit tests
│   ├── integration/   # Integration tests
│   └── fixtures/      # Test data
├── config/
│   ├── dev/           # Dev configuration
│   ├── test/          # Test configuration
│   └── prod/          # Prod configuration
├── docs/              # Documentation
├── scripts/           # Deployment/utility scripts
└── requirements.txt   # Dependencies
```

### 1.2 Module Organization
- One transformation per file
- Clear separation of concerns
- Logical grouping of related functions
- Maximum file size: 500 lines

---

## 2. NAMING CONVENTIONS

### 2.1 File Names
```python
# Good
customer_transformation.py
order_aggregation.py
data_quality_checks.py

# Bad
custTrans.py
order-agg.py
DQChecks.py
```

### 2.2 Variable Names
```python
# Good - descriptive, lowercase with underscores
customer_df = spark.read.table("customers")
total_order_amount = df.groupBy("customer_id").sum("amount")
is_active = col("status") == "ACTIVE"

# Bad - ambiguous, camelCase
custDF = spark.read.table("customers")
totAmt = df.groupBy("customer_id").sum("amount")
isAct = col("status") == "ACTIVE"
```

### 2.3 Function Names
```python
# Good - verb phrases, descriptive
def load_customer_data(source_path: str) -> DataFrame:
    pass

def apply_business_rules(df: DataFrame) -> DataFrame:
    pass

def validate_data_quality(df: DataFrame) -> Dict[str, Any]:
    pass

# Bad - ambiguous, too short
def load(path):
    pass

def apply(df):
    pass

def validate(df):
    pass
```

### 2.4 Class Names
```python
# Good - PascalCase, descriptive nouns
class CustomerTransformation:
    pass

class DataQualityValidator:
    pass

class OrderAggregator:
    pass

# Bad
class customer_transformation:
    pass

class DQVal:
    pass
```

### 2.5 Constants
```python
# Good - uppercase with underscores
MAX_RETRY_ATTEMPTS = 3
DEFAULT_PARTITION_SIZE = 1000000
VALID_STATUS_CODES = ["ACTIVE", "PENDING", "COMPLETED"]

# Bad
maxRetry = 3
defaultPartSize = 1000000
```

---

## 3. CODE FORMATTING

### 3.1 Line Length
- Maximum 100 characters per line
- Break long chains into multiple lines

```python
# Good
result_df = (
    source_df
    .filter(col("status") == "ACTIVE")
    .withColumn("processed_date", current_date())
    .select("customer_id", "order_id", "amount", "processed_date")
)

# Bad
result_df = source_df.filter(col("status") == "ACTIVE").withColumn("processed_date", current_date()).select("customer_id", "order_id", "amount", "processed_date")
```

### 3.2 Indentation
- Use 4 spaces (no tabs)
- Consistent indentation levels

```python
# Good
def process_orders(df: DataFrame) -> DataFrame:
    validated_df = (
        df
        .filter(col("amount") > 0)
        .filter(col("order_date").isNotNull())
    )
    
    return validated_df

# Bad
def process_orders(df: DataFrame) -> DataFrame:
  validated_df = (
      df
    .filter(col("amount") > 0)
      .filter(col("order_date").isNotNull())
  )
  
  return validated_df
```

### 3.3 Blank Lines
- Two blank lines between top-level functions/classes
- One blank line between methods

```python
# Good
class DataProcessor:
    def __init__(self):
        pass
    
    def process(self):
        pass


def standalone_function():
    pass


# Bad
class DataProcessor:
    def __init__(self):
        pass
    def process(self):
        pass
def standalone_function():
    pass
```

---

## 4. DOCUMENTATION

### 4.1 Module Docstrings
```python
"""
Customer Data Transformation Module

This module contains transformations for customer data processing,
including data cleansing, enrichment, and aggregation.

Functions:
    load_customer_data: Loads customer data from source
    apply_business_rules: Applies business transformation rules
    aggregate_customer_metrics: Calculates customer-level metrics

Author: Data Engineering Team
Last Updated: 2024-01-01
"""
```

### 4.2 Function Docstrings
```python
def apply_business_rules(
    df: DataFrame,
    effective_date: str,
    include_inactive: bool = False
) -> DataFrame:
    """
    Apply business transformation rules to customer data.
    
    This function applies standard business rules including:
    - Data quality validations
    - Status derivations
    - Business calculations
    
    Args:
        df: Input DataFrame containing customer data
        effective_date: Processing date in YYYY-MM-DD format
        include_inactive: Whether to include inactive customers
        
    Returns:
        DataFrame with business rules applied
        
    Raises:
        ValueError: If effective_date format is invalid
        DataQualityException: If critical quality checks fail
        
    Example:
        >>> customer_df = spark.read.table("customers")
        >>> result_df = apply_business_rules(customer_df, "2024-01-01")
        >>> result_df.count()
        1000
    """
    pass
```

### 4.3 Inline Comments
```python
# Good - explain why, not what
# Apply 10% discount for premium customers (business rule BR-123)
premium_discount = when(col("customer_tier") == "PREMIUM", col("amount") * 0.9)

# Bad - states the obvious
# Multiply amount by 0.9
premium_discount = when(col("customer_tier") == "PREMIUM", col("amount") * 0.9)
```

---

## 5. ERROR HANDLING

### 5.1 Exception Handling
```python
# Good - specific exceptions, informative messages
def load_data(path: str) -> DataFrame:
    try:
        df = spark.read.parquet(path)
        logger.info(f"Successfully loaded {df.count()} records from {path}")
        return df
    except AnalysisException as e:
        logger.error(f"Failed to read from {path}: {str(e)}")
        raise DataLoadException(f"Invalid path or file format: {path}") from e
    except Exception as e:
        logger.error(f"Unexpected error loading data: {str(e)}")
        raise

# Bad - catching all exceptions, no logging
def load_data(path):
    try:
        return spark.read.parquet(path)
    except:
        return None
```

### 5.2 Custom Exceptions
```python
class DataQualityException(Exception):
    """Raised when data quality checks fail"""
    pass

class ConfigurationException(Exception):
    """Raised when configuration is invalid"""
    pass

class TransformationException(Exception):
    """Raised when transformation logic fails"""
    pass
```

### 5.3 Validation
```python
# Good - validate inputs early
def process_date_range(start_date: str, end_date: str) -> DataFrame:
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required")
    
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {str(e)}")
    
    if start > end:
        raise ValueError("start_date must be before end_date")
    
    # Process data
    pass
```

---

## 6. LOGGING

### 6.1 Logging Configuration
```python
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add Spark logging
spark = SparkSession.builder.appName("MyApp").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
```

### 6.2 Logging Levels
```python
# DEBUG - detailed diagnostic information
logger.debug(f"Processing partition {partition_id} with {record_count} records")

# INFO - general informational messages
logger.info(f"Starting customer data load from {source_path}")
logger.info(f"Successfully processed {total_records} records in {duration}s")

# WARNING - potentially harmful situations
logger.warning(f"Found {null_count} records with null customer_id")
logger.warning(f"Performance degradation: processing took {duration}s (expected <{sla}s)")

# ERROR - error events that might still allow continued execution
logger.error(f"Failed to process batch {batch_id}: {str(error)}")

# CRITICAL - severe error events
logger.critical(f"Database connection lost. Unable to continue processing")
```

### 6.3 Structured Logging
```python
# Good - include context
logger.info(
    "Data load completed",
    extra={
        "source_path": source_path,
        "record_count": df.count(),
        "duration_seconds": duration,
        "job_id": job_id
    }
)

# Bad - unstructured
logger.info("Done")
```

---

## 7. PERFORMANCE OPTIMIZATION

### 7.1 DataFrame Operations
```python
# Good - cache when reusing DataFrames
customer_df = spark.read.table("customers").cache()
result1 = customer_df.filter(col("status") == "ACTIVE")
result2 = customer_df.filter(col("status") == "INACTIVE")

# Good - use filter pushdown
df = spark.read.parquet(path).filter(col("date") >= "2024-01-01")

# Good - avoid collect() on large datasets
# Instead use take() or write to storage
sample_data = df.take(100)

# Bad - collecting large datasets
all_data = df.collect()  # Avoid!
```

### 7.2 Partitioning
```python
# Good - repartition before expensive operations
df = (
    source_df
    .repartition(200, "customer_id")
    .groupBy("customer_id")
    .agg(sum("amount").alias("total_amount"))
)

# Good - coalesce when reducing partitions
df.coalesce(10).write.parquet(output_path)

# Bad - too many small partitions
df.repartition(10000).write.parquet(output_path)
```

### 7.3 Broadcast Joins
```python
from pyspark.sql.functions import broadcast

# Good - broadcast small dimension tables
result = (
    large_fact_df
    .join(broadcast(small_dim_df), "customer_id")
)

# Bad - regular join with small dimension
result = large_fact_df.join(small_dim_df, "customer_id")
```

### 7.4 Column Selection
```python
# Good - select only needed columns early
df = (
    spark.read.table("large_table")
    .select("customer_id", "order_id", "amount")
    .filter(col("amount") > 1000)
)

# Bad - selecting all columns then filtering
df = (
    spark.read.table("large_table")
    .filter(col("amount") > 1000)
)
```

---

## 8. DATA QUALITY

### 8.1 Null Checks
```python
# Good - explicit null handling
result_df = (
    df
    .withColumn(
        "cleaned_amount",
        when(col("amount").isNull(), 0)
        .otherwise(col("amount"))
    )
    .filter(col("customer_id").isNotNull())
)
```

### 8.2 Data Validation
```python
def validate_customer_data(df: DataFrame) -> Dict[str, Any]:
    """Validate customer data quality"""
    total_count = df.count()
    null_customer_id = df.filter(col("customer_id").isNull()).count()
    duplicate_count = df.groupBy("customer_id").count().filter(col("count") > 1).count()
    invalid_email = df.filter(~col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$')).count()
    
    validation_results = {
        "total_records": total_count,
        "null_customer_id": null_customer_id,
        "duplicate_customers": duplicate_count,
        "invalid_emails": invalid_email,
        "null_percentage": (null_customer_id / total_count * 100) if total_count > 0 else 0
    }
    
    # Raise exception if critical thresholds exceeded
    if validation_results["null_percentage"] > 5:
        raise DataQualityException(
            f"Null customer_id percentage {validation_results['null_percentage']:.2f}% "
            f"exceeds threshold of 5%"
        )
    
    return validation_results
```

### 8.3 Schema Validation
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# Define expected schema
expected_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("registration_date", DateType(), False),
    StructField("status", StringType(), False)
])

def validate_schema(df: DataFrame, expected_schema: StructType) -> bool:
    """Validate DataFrame schema matches expected schema"""
    if df.schema != expected_schema:
        actual_fields = set(df.schema.fieldNames())
        expected_fields = set(expected_schema.fieldNames())
        
        missing = expected_fields - actual_fields
        extra = actual_fields - expected_fields
        
        error_msg = []
        if missing:
            error_msg.append(f"Missing fields: {missing}")
        if extra:
            error_msg.append(f"Extra fields: {extra}")
        
        raise DataQualityException("; ".join(error_msg))
    
    return True
```

---

## 9. CONFIGURATION MANAGEMENT

### 9.1 Externalize Configuration
```python
# config/config.py
from dataclasses import dataclass
from typing import Dict
import json

@dataclass
class SourceConfig:
    path: str
    format: str
    options: Dict[str, str]

@dataclass
class TargetConfig:
    path: str
    format: str
    mode: str
    partition_by: list

class ConfigManager:
    def __init__(self, config_path: str, environment: str):
        with open(config_path) as f:
            config = json.load(f)
        self.config = config[environment]
    
    def get_source_config(self, source_name: str) -> SourceConfig:
        source = self.config["sources"][source_name]
        return SourceConfig(**source)
    
    def get_target_config(self, target_name: str) -> TargetConfig:
        target = self.config["targets"][target_name]
        return TargetConfig(**target)

# Usage
config = ConfigManager("config/config.json", "prod")
source_config = config.get_source_config("customers")
```

### 9.2 Environment-Specific Configuration
```json
// config/dev/config.json
{
  "dev": {
    "sources": {
      "customers": {
        "path": "s3://dev-bucket/customers",
        "format": "parquet",
        "options": {"mergeSchema": "true"}
      }
    },
    "targets": {
      "processed_customers": {
        "path": "s3://dev-bucket/processed/customers",
        "format": "delta",
        "mode": "overwrite",
        "partition_by": ["processing_date"]
      }
    },
    "spark_config": {
      "spark.sql.shuffle.partitions": "100",
      "spark.executor.memory": "4g"
    }
  }
}
```

---

## 10. TESTING

### 10.1 Unit Test Structure
```python
import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from transformations.customer_transformation import apply_business_rules

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def test_apply_business_rules_filters_inactive(spark):
    # Arrange
    input_data = [
        ("C001", "Active Customer", "ACTIVE"),
        ("C002", "Inactive Customer", "INACTIVE")
    ]
    input_df = spark.createDataFrame(input_data, ["customer_id", "name", "status"])
    
    expected_data = [
        ("C001", "Active Customer", "ACTIVE")
    ]
    expected_df = spark.createDataFrame(expected_data, ["customer_id", "name", "status"])
    
    # Act
    result_df = apply_business_rules(input_df, include_inactive=False)
    
    # Assert
    assert_df_equality(result_df, expected_df)

def test_apply_business_rules_handles_nulls(spark):
    # Test null handling
    input_data = [
        ("C001", None, "ACTIVE"),
        ("C002", "Customer", "ACTIVE")
    ]
    input_df = spark.createDataFrame(input_data, ["customer_id", "name", "status"])
    
    result_df = apply_business_rules(input_df)
    
    # Verify no nulls in result
    assert result_df.filter(col("name").isNull()).count() == 0
```

### 10.2 Integration Test Pattern
```python
def test_end_to_end_customer_processing(spark, tmp_path):
    # Setup
    source_path = str(tmp_path / "source")
    target_path = str(tmp_path / "target")
    
    # Create test data
    test_data = [
        ("C001", "Customer 1", "2024-01-01", "ACTIVE"),
        ("C002", "Customer 2", "2024-01-02", "ACTIVE")
    ]
    test_df = spark.createDataFrame(
        test_data,
        ["customer_id", "name", "registration_date", "status"]
    )
    test_df.write.parquet(source_path)
    
    # Execute
    from jobs.customer_processing import main
    main(spark, source_path, target_path, "2024-01-01")
    
    # Verify
    result_