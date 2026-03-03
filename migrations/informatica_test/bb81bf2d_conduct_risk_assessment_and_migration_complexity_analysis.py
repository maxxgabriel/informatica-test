import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, struct, collect_list, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RiskSeverity(Enum):
    CRITICAL = 5
    HIGH = 4
    MEDIUM = 3
    LOW = 2
    NEGLIGIBLE = 1


class RiskCategory(Enum):
    TRANSFORMATION_COMPLEXITY = "Transformation Complexity"
    DATA_QUALITY = "Data Quality"
    DEPRECATED_FEATURES = "Deprecated Features"
    CUSTOM_CODE = "Custom Code"
    THIRD_PARTY_DEPENDENCIES = "Third-Party Dependencies"
    PERFORMANCE = "Performance"
    TECHNICAL_DEBT = "Technical Debt"
    COMPLIANCE = "Compliance"


class ComplexityLevel(Enum):
    SIMPLE = 1
    MODERATE = 2
    COMPLEX = 3
    VERY_COMPLEX = 4
    CRITICAL = 5


@dataclass
class RiskItem:
    risk_id: str
    category: str
    description: str
    severity: int
    impact: str
    likelihood: str
    affected_modules: List[str]
    mitigation_strategy: str
    contingency_plan: str
    owner: str
    target_resolution_date: str
    status: str
    created_date: str
    updated_date: str


@dataclass
class TransformationComplexity:
    module_name: str
    transformation_type: str
    complexity_score: int
    complexity_level: str
    number_of_sources: int
    number_of_targets: int
    number_of_transformations: int
    number_of_expressions: int
    number_of_lookups: int
    number_of_joins: int
    has_custom_code: bool
    has_stored_procedures: bool
    estimated_effort_hours: int
    migration_approach: str
    technical_notes: str


@dataclass
class DeprecatedFeature:
    feature_name: str
    usage_count: int
    affected_workflows: List[str]
    replacement_strategy: str
    migration_complexity: str
    estimated_effort: int


@dataclass
class DataQualityCheck:
    check_id: str
    check_name: str
    check_type: str
    source_table: str
    column_name: Optional[str]
    validation_rule: str
    threshold: str
    severity: str
    pyspark_implementation: str


@dataclass
class PerformanceBottleneck:
    bottleneck_id: str
    module_name: str
    issue_description: str
    current_performance: str
    target_performance: str
    optimization_strategy: str
    pyspark_optimization: str
    estimated_improvement: str


@dataclass
class TechnicalDebt:
    debt_id: str
    description: str
    workaround_type: str
    business_impact: str
    recommended_fix: str
    effort_estimate: int
    priority: str


class InformaticaMigrationRiskAssessment:
    """
    Comprehensive risk assessment and migration complexity analysis tool
    for Informatica PowerCenter to PySpark migration
    """

    def __init__(self, spark: SparkSession, assessment_config: Dict[str, Any]):
        self.spark = spark
        self.config = assessment_config
        self.assessment_date = datetime.now().isoformat()
        self.risk_register: List[RiskItem] = []
        self.complexity_register: List[TransformationComplexity] = []
        self.deprecated_features: List[DeprecatedFeature] = []
        self.data_quality_checks: List[DataQualityCheck] = []
        self.performance_bottlenecks: List[PerformanceBottleneck] = []
        self.technical_debt_items: List[TechnicalDebt] = []

    def assess_transformation_complexity(self, metadata_df: DataFrame) -> DataFrame:
        """
        Analyze transformation complexity from Informatica metadata
        """
        logger.info("Assessing transformation complexity...")

        complexity_rules = """
        Complexity Scoring Rules:
        - Simple (1-5 points): Direct mappings, simple filters
        - Moderate (6-10 points): Basic aggregations, simple lookups
        - Complex (11-20 points): Multiple joins, complex expressions, stored procedures
        - Very Complex (21-35 points): Dynamic transformations, complex business logic
        - Critical (36+ points): Cascading dependencies, performance-critical paths
        """

        def calculate_complexity_score(row):
            score = 0
            score += row['number_of_transformations'] * 2
            score += row['number_of_expressions'] * 1.5
            score += row['number_of_lookups'] * 3
            score += row['number_of_joins'] * 2.5
            score += 10 if row['has_custom_code'] else 0
            score += 8 if row['has_stored_procedures'] else 0
            score += row['number_of_sources'] * 1
            score += row['number_of_targets'] * 1

            if score <= 5:
                level = ComplexityLevel.SIMPLE
                effort = score * 2
                approach = "Direct translation using PySpark DataFrame API"
            elif score <= 10:
                level = ComplexityLevel.MODERATE
                effort = score * 3
                approach = "Standard PySpark patterns with moderate refactoring"
            elif score <= 20:
                level = ComplexityLevel.COMPLEX
                effort = score * 4
                approach = "Complex PySpark logic with UDFs and optimizations"
            elif score <= 35:
                level = ComplexityLevel.VERY_COMPLEX
                effort = score * 5
                approach = "Architectural redesign with advanced PySpark features"
            else:
                level = ComplexityLevel.CRITICAL
                effort = score * 6
                approach = "Full reengineering with performance tuning and testing"

            return TransformationComplexity(
                module_name=row['module_name'],
                transformation_type=row['transformation_type'],
                complexity_score=int(score),
                complexity_level=level.name,
                number_of_sources=row['number_of_sources'],
                number_of_targets=row['number_of_targets'],
                number_of_transformations=row['number_of_transformations'],
                number_of_expressions=row['number_of_expressions'],
                number_of_lookups=row['number_of_lookups'],
                number_of_joins=row['number_of_joins'],
                has_custom_code=row['has_custom_code'],
                has_stored_procedures=row['has_stored_procedures'],
                estimated_effort_hours=int(effort),
                migration_approach=approach,
                technical_notes=""
            )

        complexity_items = []
        for row in metadata_df.collect():
            complexity_item = calculate_complexity_score(row)
            complexity_items.append(asdict(complexity_item))
            self.complexity_register.append(complexity_item)

        complexity_df = self.spark.createDataFrame(complexity_items)

        logger.info(f"Assessed {len(complexity_items)} transformation modules")
        return complexity_df

    def identify_deprecated_features(self, metadata_df: DataFrame) -> DataFrame:
        """
        Identify deprecated PowerCenter features that need special handling
        """
        logger.info("Identifying deprecated PowerCenter features...")

        deprecated_mappings = {
            'PowerMart': {
                'replacement': 'Native PySpark SQL and DataFrame operations',
                'complexity': 'HIGH',
                'effort': 40
            },
            'XML_Generator': {
                'replacement': 'PySpark to_json() and custom XML writers',
                'complexity': 'MEDIUM',
                'effort': 24
            },
            'Normalizer': {
                'replacement': 'PySpark explode() and flatMap() operations',
                'complexity': 'MEDIUM',
                'effort': 16
            },
            'Update_Strategy': {
                'replacement': 'Delta Lake merge operations or custom upsert logic',
                'complexity': 'HIGH',
                'effort': 32
            },
            'Sequence_Generator': {
                'replacement': 'monotonically_increasing_id() or row_number() window function',
                'complexity': 'LOW',
                'effort': 8
            },
            'Stored_Procedure': {
                'replacement': 'PySpark UDFs or external API calls',
                'complexity': 'VERY_HIGH',
                'effort': 60
            },
            'External_Procedure': {
                'replacement': 'PySpark subprocess calls or REST API integration',
                'complexity': 'HIGH',
                'effort': 48
            },
            'Java_Transformation': {
                'replacement': 'PySpark UDFs or Pandas UDFs',
                'complexity': 'VERY_HIGH',
                'effort': 56
            },
            'Unstructured_Data': {
                'replacement': 'PySpark binary file readers and custom parsers',
                'complexity': 'VERY_HIGH',
                'effort': 64
            },
            'PowerExchange': {
                'replacement': 'Native connectors or custom JDBC/REST integration',
                'complexity': 'CRITICAL',
                'effort': 80
            }
        }

        deprecated_features = []

        for feature, details in deprecated_mappings.items():
            usage_df = metadata_df.filter(col('component_type').contains(feature))
            usage_count = usage_df.count()

            if usage_count > 0:
                affected_workflows = [row['workflow_name'] for row in usage_df.select('workflow_name').distinct().collect()]

                deprecated_item = DeprecatedFeature(
                    feature_name=feature,
                    usage_count=usage_count,
                    affected_workflows=affected_workflows,
                    replacement_strategy=details['replacement'],
                    migration_complexity=details['complexity'],
                    estimated_effort=details['effort'] * usage_count
                )
                deprecated_features.append(asdict(deprecated_item))
                self.deprecated_features.append(deprecated_item)

                self._add_risk(
                    category=RiskCategory.DEPRECATED_FEATURES,
                    description=f"Deprecated feature '{feature}' found in {usage_count} locations",
                    severity=RiskSeverity.HIGH if details['complexity'] in ['HIGH', 'VERY_HIGH', 'CRITICAL'] else RiskSeverity.MEDIUM,
                    affected_modules=affected_workflows,
                    mitigation_strategy=details['replacement']
                )

        if deprecated_features:
            deprecated_df = self.spark.createDataFrame(deprecated_features)
        else:
            schema = StructType([
                StructField("feature_name", StringType(), True),
                StructField("usage_count", IntegerType(), True),
                StructField("affected_workflows", ArrayType(StringType()), True),
                StructField("replacement_strategy", StringType(), True),
                StructField("migration_complexity", StringType(), True),
                StructField("estimated_effort", IntegerType(), True)
            ])
            deprecated_df = self.spark.createDataFrame([], schema)

        logger.info(f"Identified {len(deprecated_features)} deprecated features")
        return deprecated_df

    def assess_data_quality_requirements(self, source_metadata: DataFrame) -> DataFrame:
        """
        Evaluate data quality and validation requirements
        """
        logger.info("Assessing data quality requirements...")

        quality_checks = [
            DataQualityCheck(
                check_id="DQ001",
                check_name="Null Value Check",
                check_type="COMPLETENESS",
                source_table="*",
                column_name=None,
                validation_rule="Non-nullable columns must not contain nulls",
                threshold="0%",
                severity="HIGH",
                pyspark_implementation="df.filter(col('column_name').isNull()).count() == 0"
            ),
            DataQualityCheck(
                check_id="DQ002",
                check_name="Duplicate Key Check",
                check_type="UNIQUENESS",
                source_table="*",
                column_name=None,
                validation_rule="Primary key columns must be unique",
                threshold="0 duplicates",
                severity="CRITICAL",
                pyspark_implementation="df.groupBy('key_column').count().filter(col('count') > 1).count() == 0"
            ),
            DataQualityCheck(
                check_id="DQ003",
                check_name="Data Type Validation",
                check_type="VALIDITY",
                source_table="*",
                column_name=None,
                validation_rule="Data types must match schema definitions",
                threshold="100% conformance",
                severity="HIGH",
                pyspark_implementation="df.select([col(c).cast(expected_type) for c in df.columns])"
            ),
            DataQualityCheck(
                check_id="DQ004",
                check_name="Referential Integrity",
                check_type="CONSISTENCY",
                source_table="*",
                column_name=None,
                validation_rule="Foreign keys must exist in parent tables",
                threshold="100% match",
                severity="HIGH",
                pyspark_implementation="df.join(parent_df, df.fk == parent_df.pk, 'left_anti').count() == 0"
            ),
            DataQualityCheck(
                check_id="DQ005",
                check_name="Business Rule Validation",
                check_type="VALIDITY",
                source_table="*",
                column_name=None,
                validation_rule="Data must satisfy business constraints",
                threshold="100% compliance",
                severity="MEDIUM",
                pyspark_implementation="df.filter(business_rule_condition).count() == df.count()"
            ),
            DataQualityCheck(
                check_id="DQ006",
                check_name="Date Range Validation",
                check_type="VALIDITY",
                source_table="*",
                column_name=None,
                validation_rule="Dates must be within valid ranges",
                threshold="100% valid dates",
                severity="MEDIUM",
                pyspark_implementation="df.filter((col('date_col') >= start_date) & (col('date_col') <= end_date))"
            ),
            DataQualityCheck(
                check_id="DQ007",
                check_name="Record Count Reconciliation",
                check_type="COMPLETENESS",
                source_table="*",
                column_name=None,
                validation_rule="Source and target counts must match",
                threshold="+/- 0.1%",
                severity="CRITICAL",
                pyspark_implementation="abs(source_df.count() - target_df.count()) / source_df.count() <= 0.001"
            ),
            DataQualityCheck(
                check_id="DQ008",
                check_name="Data Profiling Statistics",
                check_type="PROFILING",
                source_table="*",
                column_name=None,
                validation_rule="Statistical profiles must match baseline",
                threshold="Within tolerance",
                severity="LOW",
                pyspark_implementation="df.describe().show()"
            )
        ]

        self.data_quality_checks = quality_checks
        quality_df = self.spark.createDataFrame([asdict(qc) for qc in quality_checks])

        self._add_risk(
            category=RiskCategory.DATA_QUALITY,
            description="Data quality validation requirements must be implemented in PySpark",
            severity=RiskSeverity.HIGH,
            affected_modules=["All migration modules"],
            mitigation_strategy="Implement comprehensive PySpark-based data quality framework"
        )

        logger.info(f"Defined {len(quality_checks)} data quality checks")
        return quality_df

    def identify_performance_bottlenecks(self, performance_metadata: DataFrame) -> DataFrame:
        """
        Identify performance bottlenecks and optimization needs
        """
        logger.info("Identifying performance bottlenecks...")

        bottlenecks = [
            PerformanceBottleneck(
                bottleneck_id="PERF001",
                module_name="Large Lookup Transformations",
                issue_description="Connected lookups with large cache causing memory issues",
                current_performance=">60 min execution time",
                target_performance="<15 min execution time",
                optimization_strategy="Convert to broadcast joins or bucketed joins",
                pyspark_optimization="df.join(broadcast(lookup_df), join_condition) or bucketing",
                estimated_improvement="75% reduction"
            ),
            PerformanceBottleneck(
                bottleneck_id="PERF002",
                module_name="Sequential Processing",
                issue_description="Sequential workflow execution limiting throughput",
                current_performance="Single-threaded processing",
                target_performance="Parallel distributed processing",
                optimization_strategy="Leverage Spark's distributed computing",
                pyspark_optimization="Natural parallelism with appropriate partitioning",
                estimated_improvement="10x-50x improvement"
            ),
            PerformanceBottleneck(
                bottleneck_id="PERF003",
                module_name="Full Table Scans",
                issue_description="Unpartitioned tables requiring full scans",
                current_performance="Scanning entire tables",
                target_performance="Partition pruning and predicate pushdown",
                optimization_strategy="Implement partitioning strategy",
                pyspark_optimization="Partition by date/region and use partition filters",
                estimated_improvement="80-95% data scan reduction"
            ),
            PerformanceBottleneck(
                bottleneck_id="PERF004",
                module_name="Aggregation Operations",
                issue_description="Non-optimized aggregations causing spills",
                current_performance="Memory spills and slow aggregations",
                target_performance="In-memory aggregations",
                optimization_strategy="Optimize shuffle partitions and memory allocation",
                pyspark_optimization="Configure spark.sql.shuffle.partitions appropriately",
                estimated_improvement="60% reduction"
            ),
            PerformanceBottleneck(
                bottleneck_id="PERF005",
                module_name="Complex Expressions",
                issue_description="Row-by-row expression evaluation inefficiency",
                current_performance="Slow expression processing",
                target_performance="Vectorized operations",
                optimization_strategy="Use Pandas UDFs for vectorized processing",
                pyspark_optimization="@pandas_udf decorator for vectorized operations",
                estimated_improvement="50-70% improvement"
            ),
            PerformanceBottleneck(
                bottleneck_id="PERF006",
                module_name="File I/O Operations",
                issue_description="Small file problems and inefficient formats",
                current_performance="Many small files, CSV format",
                target_performance="Optimized file size and format",
                optimization_strategy="Use Parquet/ORC with optimal file sizes",
                pyspark_optimization="Coalesce/repartition before writing, use columnar formats",
                estimated_improvement="70-90% I/O improvement"
            )
        ]

        self.performance_bottlenecks = bottlenecks
        performance_df = self.spark.createDataFrame([asdict(pb) for pb in bottlenecks])

        self._add_risk(
            category=RiskCategory.PERFORMANCE,
            description="Multiple performance bottlenecks identified requiring optimization",
            severity=RiskSeverity.HIGH,
            affected_modules=["Multiple workflows"],
            mitigation_strategy="Implement PySpark performance best practices and tuning"
        )

        logger.info(f"Identified {len(bottlenecks)} performance bottlenecks")
        return performance_df

    def document_technical_debt(self, code_analysis_df: DataFrame) -> DataFrame:
        """
        Document technical debt and workarounds
        """
        logger.info("Documenting technical debt...")

        debt_items = [
            TechnicalDebt(
                debt_id="TD001",
                description="Hard-coded connection strings and credentials",
                workaround_type="Security Risk",
                business_impact="HIGH - Security vulnerability, compliance issues",
                recommended_fix="Implement secure credential management (AWS Secrets Manager, Azure Key Vault)",
                effort_estimate=16,
                priority="CRITICAL"
            ),
            TechnicalDebt(
                debt_id="TD002",
                description="Magic numbers and undocumented business rules in expressions",
                workaround_type="Maintainability Issue",
                business_impact="MEDIUM - Difficult to maintain and understand logic",
                recommended_fix="Extract to configuration files with documentation",
                effort_estimate=24,
                priority="HIGH"
            ),
            TechnicalDebt(
                debt_id="TD003",
                description="Lack of error handling and logging",
                workaround_type="Operational Risk",
                business_impact="HIGH - Difficult to troubleshoot failures",
                recommended_fix="Implement comprehensive logging and error handling framework",
                effort_estimate=40,
                priority="HIGH"
            ),
            TechnicalDebt(
                debt_id="TD004",
                description="Duplicate transformation logic across workflows",
                workaround_type="Code Duplication",
                business_impact="MEDIUM - Maintenance overhead, inconsistency risk",
                recommended_fix="Create reusable PySpark modules and functions",
                effort_estimate=32,
                priority="MEDIUM"
            ),
            TechnicalDebt(
                debt_id="TD005",
                description="No version control for mappings and workflows",
                workaround_type="Configuration Management",
                business_impact="HIGH - Change tracking and rollback difficulties",
                recommended_fix="Implement Git-based version control for all PySpark code",
                effort_estimate=8,
                priority="CRITICAL"
            ),
            TechnicalDebt(
                debt_id="TD006",
                description="Insufficient test coverage and validation",
                workaround_type="Quality Risk",
                business_impact="HIGH - Production defects and data quality issues",
                recommended_fix="Develop comprehensive unit and integration test suite",
                effort_estimate=80,
                priority="HIGH"
            ),
            TechnicalDebt(
                debt_id="TD007",
                description="Manual deployment processes",
                workaround_type="Operational Inefficiency",
                business_impact="MEDIUM - Deployment errors, time-consuming releases",
                recommended_fix="Implement CI/CD pipeline for automated deployments",
                effort_estimate=60,
                priority="MEDIUM"
            )
        ]

        self.technical_debt_items = debt_items
        debt_df = self.spark.createDataFrame([asdict(td) for td in debt_items])

        self._add_risk(
            category=RiskCategory.TECHNICAL_DEBT,
            description="Significant technical debt requiring remediation",
            severity=RiskSeverity.HIGH,
            affected_modules=["Infrastructure and all modules"],
            mitigation_strategy="Address critical debt items as part of migration"
        )

        logger.info(f"Documented {len(debt_items)} technical debt items")
        return debt_df

    def assess_custom_code_dependencies(self, code_inventory: DataFrame) -> DataFrame:
        """
        Document custom code and third-party dependencies
        """
        logger.info("Assessing custom code and dependencies...")

        custom_code_risks = [
            {
                'component_type': 'Java Transformations',
                'migration_path': 'Convert to PySpark UDFs or Pandas UDFs',
                'complexity': 'HIGH',
                'risk_level': 'HIGH',
                'example_implementation': '''
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def custom_transformation(input_value):
    # Converted Java logic
    result = complex_business_logic(input_value)
    return result

df_transformed = df.withColumn('output_col', custom_transformation(col('input_col')))
                '''
            },
            {
                'component_type': 'Stored Procedures',
                'migration_path': 'Rewrite as PySpark DataFrame operations or use JDBC for legacy systems',
                'complexity': 'VERY_HIGH',
                'risk_level': 'CRITICAL',
                'example_implementation': '''
# Option 1: Pure PySpark
df_result = df.groupBy('key').agg(
    sum('amount').alias('total_amount'),
    count('*').alias('record_count')
).filter(col('total_amount') > threshold)

# Option 2: Call stored procedure if necessary
jdbc_url = "jdbc:oracle:thin:@//host:port/service"
sp_df = spark.read.jdbc(
    url=jdbc_url,
    table="(CALL stored_procedure(?))",
    properties=connection_properties
)
                '''
            },
            {
                'component_type': 'External Scripts',
                'migration_path': 'Integrate using subprocess or REST APIs',
                'complexity': 'MEDIUM',
                'risk_level': 'MEDIUM',
                'example_implementation': '''
import subprocess
from pyspark.sql.functions import pandas_udf

@pandas_udf('string')
def call_external_script(input_series):
    results = []
    for value in input_series:
        result = subprocess.check_output(['python', 'external_script.py', value])
        results.append(result.decode('utf-8'))
    return pd.Series(results)
                '''
            },
            {
                'component_type': 'Third-Party Connectors',
                'migration_path': 'Use Spark connectors or implement custom readers',
                'complexity': 'MEDIUM',
                'risk_level': 'MEDIUM',
                'example_implementation': '''
# Using Spark connector
df = spark.read \\
    .format("jdbc") \\
    .option("driver", "com.thirdparty.Driver") \\
    .option("url", connection_url) \\
    .load()

# Custom reader for proprietary formats
from pyspark.sql import Row
rdd = spark.sparkContext.binaryFiles(path).map(parse_proprietary_format)
df = spark.createDataFrame(rdd)
                '''
            }
        ]

        custom_code_df = self.spark.createDataFrame(custom_code_risks)

        self._add_risk(
            category=RiskCategory.CUSTOM_CODE,
            description="Custom code and third-party dependencies require careful migration",
            severity=RiskSeverity.HIGH,
            affected_modules=["Modules with custom components"],
            mitigation_strategy="Develop migration patterns and test thoroughly"
        )

        logger.info(f"Assessed {len(custom_code_risks)} custom code components")
        return custom_code_df

    def _add_risk(
            self,
            category: RiskCategory,
            description: str,
            severity: RiskSeverity,
            affected_modules: List[str],
            mitigation_strategy: str,
            contingency_plan: str = "Escalate to technical leadership for resolution",
            owner: str = "Migration Team",
            target_resolution_date: str = ""
    ):
        """Helper method to add risk to register"""
        risk_id = f"RISK-{len(self.risk_register) + 1:04d}"

        impact_mapping = {
            RiskSeverity.CRITICAL: "Migration blocker - immediate resolution required",
            RiskSeverity.HIGH: "Significant impact on timeline or quality",
            RiskSeverity.MEDIUM: "Moderate impact manageable with resources",
            RiskSeverity.LOW: "Minor impact with minimal mitigation",
            RiskSeverity.NEGLIGIBLE: "Negligible impact"
        }

        likelihood_mapping = {
            RiskSeverity.CRITICAL: "Very High (>80%)",
            RiskSeverity.HIGH: "High (60-80%)",
            RiskSeverity.MEDIUM: "Medium (40-60%)",
            RiskSeverity.LOW: "Low (20-40%)",
            RiskSeverity.NEGLIGIBLE: "Very Low (<20%)"
        }

        risk_item = RiskItem(
            risk_id=risk_id,
            category=category.value,
            description=description,
            severity=severity.value,
            impact=impact_mapping[severity],
            likelihood=likelihood_mapping[severity],
            affected_modules=affected_modules,
            mitigation_strategy=mitigation_strategy,
            contingency_plan=contingency_plan,
            owner=owner,
            target_resolution_date=target_resolution_date or self._calculate_target_date(severity),
            status="IDENTIFIED",
            created_date=self.assessment_date,
            updated_date=self.assessment_date
        )

        self.risk_register.append(risk_item)

    def _calculate_target_date(self, severity: RiskSeverity) -> str:
        """Calculate target resolution date based on severity"""
        from datetime import datetime, timedelta

        days_mapping = {
            RiskSeverity.CRITICAL: 7,
            RiskSeverity.HIGH: 14,
            RiskSeverity.MEDIUM: 30,
            RiskSeverity.LOW: 60,
            RiskSeverity.NEGLIGIBLE: 90
        }

        target_date = datetime.now() + timedelta(days=days_mapping[severity])
        return target_date.strftime("%Y-%m-%d")

    def generate_risk_register(self) -> DataFrame:
        """Generate comprehensive risk register"""
        logger.info("Generating risk register...")

        if not self.risk_register:
            schema = StructType([
                StructField("risk_id", StringType(), False),
                StructField("category", StringType(), False),
                StructField("description", StringType(), False),
                StructField("severity", IntegerType(), False),
                StructField("impact", StringType(), False),
                StructField("likelihood", StringType(), False),
                StructField("affected_modules", ArrayType(StringType()), False),
                StructField("mitigation_strategy", StringType(), False),
                StructField("contingency_plan", StringType(), False),
                StructField("owner", StringType(), False),
                StructField("target_resolution_date", StringType(), False),
                StructField("status", StringType(), False),
                StructField("created_date", StringType(), False),
                StructField("updated_date", StringType(), False)
            ])
            return self.spark.createDataFrame([], schema)

        risk_df = self.spark.createDataFrame([asdict(risk) for risk in self.risk_register])

        logger.info(f"Generated risk register with {len(self.risk_register)} risks")
        return risk_df

    def calculate_migration_metrics(self) -> Dict[str, Any]:
        """Calculate overall migration complexity and readiness metrics"""
        logger.info("Calculating migration metrics...")

        total_complexity_score = sum(c.complexity_score for c in self.complexity_register)
        total_effort_hours = sum(c.estimated_effort_hours for c in self.complexity_register)
        total_effort_hours += sum(d.estimated_effort for d in self.deprecated_features)
        total_effort_hours += sum(td.effort_estimate for td in self.technical_debt_items)

        critical_risks = len([r for r in self.risk_register if r.severity >= RiskSeverity.HIGH.value])
        medium_risks = len([r for r in self.risk_register if r.severity == RiskSeverity.MEDIUM.value])
        low_risks = len([r for r in self.risk_register if r.severity <= RiskSeverity.LOW.value])

        complexity_distribution = {
            'SIMPLE': len([c for c in self.complexity_register if c.complexity_level == 'SIMPLE']),
            'MODERATE': len([c for c in self.complexity_register if c.complexity_level == 'MODERATE']),
            'COMPLEX': len([c for c in self.complexity_register if c.complexity_level == 'COMPLEX']),
            'VERY_