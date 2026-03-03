"""
Risk Assessment and Migration Complexity Analysis Framework
PySpark-based migration assessment tool for Informatica to PySpark conversion

This module provides comprehensive risk assessment, complexity analysis, and
migration readiness evaluation for PowerCenter to PySpark migrations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, count, countDistinct, sum as _sum, avg, max as _max,
    min as _min, coalesce, concat_ws, struct, collect_list, explode,
    current_timestamp, md5, concat, array, create_map, length, regexp_extract,
    upper, lower, trim, to_json, from_json, schema_of_json, size, split
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, ArrayType, MapType, BooleanType, LongType, DateType
)
from pyspark.sql.window import Window
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime
import json
import logging
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RiskSeverity(Enum):
    """Risk severity levels"""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFORMATIONAL = "INFORMATIONAL"


class ComplexityLevel(Enum):
    """Complexity scoring levels"""
    VERY_HIGH = 5
    HIGH = 4
    MEDIUM = 3
    LOW = 2
    VERY_LOW = 1


class MigrationPhase(Enum):
    """Migration phases"""
    ASSESSMENT = "ASSESSMENT"
    PLANNING = "PLANNING"
    DEVELOPMENT = "DEVELOPMENT"
    TESTING = "TESTING"
    DEPLOYMENT = "DEPLOYMENT"


@dataclass
class RiskItem:
    """Data class for risk items"""
    risk_id: str
    category: str
    description: str
    severity: str
    impact: str
    probability: str
    affected_components: List[str]
    mitigation_strategy: str
    contingency_plan: str
    owner: str
    target_resolution_date: str
    status: str
    dependencies: List[str]
    estimated_effort_hours: int
    business_impact: str
    technical_impact: str


@dataclass
class ComplexityScore:
    """Data class for complexity scoring"""
    component_id: str
    component_name: str
    component_type: str
    transformation_complexity: int
    data_volume_complexity: int
    business_logic_complexity: int
    integration_complexity: int
    performance_complexity: int
    overall_complexity: int
    complexity_level: str
    estimated_effort_days: int
    recommended_approach: str
    special_considerations: str


class MigrationRiskAssessor:
    """
    Comprehensive risk assessment and complexity analysis framework
    for Informatica PowerCenter to PySpark migration
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the Risk Assessor
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary containing assessment parameters
        """
        self.spark = spark
        self.config = config
        self.assessment_timestamp = datetime.now()
        
        # Risk thresholds
        self.risk_thresholds = {
            'transformation_count': 100,
            'source_count': 20,
            'data_volume_tb': 10,
            'custom_code_lines': 1000,
            'expression_complexity': 50,
            'lookup_count': 30
        }
        
        # Deprecated features mapping
        self.deprecated_features = {
            'PMCMD': 'Use PySpark DataFrameWriter with orchestration tools',
            'Command Task': 'Use PySpark with Airflow/Databricks Jobs',
            'PowerMart Repository': 'Use Unity Catalog/Git-based version control',
            'Workflow Monitor': 'Use Spark UI/Cloud monitoring solutions',
            'Session Log': 'Use structured logging with ELK/Splunk',
            'DTM Process': 'Native Spark distributed processing'
        }
        
    def create_risk_register_schema(self) -> StructType:
        """Define schema for risk register"""
        return StructType([
            StructField("risk_id", StringType(), False),
            StructField("category", StringType(), False),
            StructField("subcategory", StringType(), True),
            StructField("description", StringType(), False),
            StructField("severity", StringType(), False),
            StructField("impact_score", IntegerType(), False),
            StructField("probability_score", IntegerType(), False),
            StructField("risk_score", IntegerType(), False),
            StructField("affected_components", ArrayType(StringType()), False),
            StructField("affected_phase", StringType(), False),
            StructField("root_cause", StringType(), True),
            StructField("mitigation_strategy", StringType(), False),
            StructField("contingency_plan", StringType(), False),
            StructField("preventive_actions", ArrayType(StringType()), True),
            StructField("owner", StringType(), False),
            StructField("assigned_date", TimestampType(), False),
            StructField("target_resolution_date", DateType(), True),
            StructField("actual_resolution_date", DateType(), True),
            StructField("status", StringType(), False),
            StructField("dependencies", ArrayType(StringType()), True),
            StructField("estimated_effort_hours", IntegerType(), True),
            StructField("actual_effort_hours", IntegerType(), True),
            StructField("business_impact", StringType(), True),
            StructField("technical_impact", StringType(), True),
            StructField("financial_impact", DoubleType(), True),
            StructField("residual_risk", StringType(), True),
            StructField("created_timestamp", TimestampType(), False),
            StructField("updated_timestamp", TimestampType(), False),
            StructField("created_by", StringType(), False),
            StructField("updated_by", StringType(), True),
            StructField("comments", ArrayType(StringType()), True),
            StructField("attachments", ArrayType(StringType()), True)
        ])
    
    def create_complexity_analysis_schema(self) -> StructType:
        """Define schema for complexity analysis"""
        return StructType([
            StructField("component_id", StringType(), False),
            StructField("component_name", StringType(), False),
            StructField("component_type", StringType(), False),
            StructField("parent_folder", StringType(), True),
            StructField("transformation_count", IntegerType(), True),
            StructField("source_count", IntegerType(), True),
            StructField("target_count", IntegerType(), True),
            StructField("lookup_count", IntegerType(), True),
            StructField("expression_complexity", IntegerType(), True),
            StructField("has_custom_code", BooleanType(), True),
            StructField("custom_code_lines", IntegerType(), True),
            StructField("data_volume_gb", DoubleType(), True),
            StructField("scd_type", IntegerType(), True),
            StructField("has_aggregations", BooleanType(), True),
            StructField("has_joins", BooleanType(), True),
            StructField("join_complexity", IntegerType(), True),
            StructField("has_pivots", BooleanType(), True),
            StructField("has_xml_parsing", BooleanType(), True),
            StructField("uses_deprecated_features", BooleanType(), True),
            StructField("deprecated_features_list", ArrayType(StringType()), True),
            StructField("transformation_complexity_score", IntegerType(), False),
            StructField("data_volume_complexity_score", IntegerType(), False),
            StructField("business_logic_complexity_score", IntegerType(), False),
            StructField("integration_complexity_score", IntegerType(), False),
            StructField("performance_complexity_score", IntegerType(), False),
            StructField("overall_complexity_score", IntegerType(), False),
            StructField("complexity_level", StringType(), False),
            StructField("estimated_effort_days", IntegerType(), False),
            StructField("recommended_approach", StringType(), False),
            StructField("special_considerations", ArrayType(StringType()), True),
            StructField("data_quality_issues", ArrayType(StringType()), True),
            StructField("performance_concerns", ArrayType(StringType()), True),
            StructField("migration_priority", StringType(), True),
            StructField("assessment_timestamp", TimestampType(), False)
        ])
    
    def assess_transformation_complexity(
        self,
        metadata_df: DataFrame
    ) -> DataFrame:
        """
        Assess complexity of transformations and business logic
        
        Args:
            metadata_df: DataFrame containing PowerCenter metadata
            
        Returns:
            DataFrame with complexity scores
        """
        logger.info("Starting transformation complexity assessment")
        
        complexity_df = metadata_df.withColumn(
            "transformation_complexity_score",
            when(col("transformation_count") > 50, 5)
            .when(col("transformation_count") > 30, 4)
            .when(col("transformation_count") > 15, 3)
            .when(col("transformation_count") > 5, 2)
            .otherwise(1)
        ).withColumn(
            "data_volume_complexity_score",
            when(col("data_volume_gb") > 1000, 5)
            .when(col("data_volume_gb") > 500, 4)
            .when(col("data_volume_gb") > 100, 3)
            .when(col("data_volume_gb") > 10, 2)
            .otherwise(1)
        ).withColumn(
            "business_logic_complexity_score",
            when(
                (col("expression_complexity") > 100) |
                (col("custom_code_lines") > 1000) |
                (col("scd_type") == 3),
                5
            )
            .when(
                (col("expression_complexity") > 50) |
                (col("custom_code_lines") > 500) |
                (col("scd_type") == 2),
                4
            )
            .when(
                (col("expression_complexity") > 20) |
                (col("has_custom_code") == True),
                3
            )
            .when(col("expression_complexity") > 10, 2)
            .otherwise(1)
        ).withColumn(
            "integration_complexity_score",
            when(
                (col("source_count") > 10) |
                (col("target_count") > 5) |
                (col("has_xml_parsing") == True),
                5
            )
            .when(
                (col("source_count") > 5) |
                (col("target_count") > 3),
                4
            )
            .when(col("source_count") > 3, 3)
            .when(col("source_count") > 1, 2)
            .otherwise(1)
        ).withColumn(
            "performance_complexity_score",
            when(
                (col("lookup_count") > 20) |
                (col("join_complexity") > 10) |
                (col("data_volume_gb") > 1000),
                5
            )
            .when(
                (col("lookup_count") > 10) |
                (col("join_complexity") > 5),
                4
            )
            .when(col("lookup_count") > 5, 3)
            .when(col("has_joins") == True, 2)
            .otherwise(1)
        )
        
        # Calculate overall complexity
        complexity_df = complexity_df.withColumn(
            "overall_complexity_score",
            (
                col("transformation_complexity_score") +
                col("data_volume_complexity_score") +
                col("business_logic_complexity_score") +
                col("integration_complexity_score") +
                col("performance_complexity_score")
            ) / 5
        ).withColumn(
            "complexity_level",
            when(col("overall_complexity_score") >= 4.5, "VERY_HIGH")
            .when(col("overall_complexity_score") >= 3.5, "HIGH")
            .when(col("overall_complexity_score") >= 2.5, "MEDIUM")
            .when(col("overall_complexity_score") >= 1.5, "LOW")
            .otherwise("VERY_LOW")
        ).withColumn(
            "estimated_effort_days",
            when(col("complexity_level") == "VERY_HIGH", col("transformation_count") * 3)
            .when(col("complexity_level") == "HIGH", col("transformation_count") * 2)
            .when(col("complexity_level") == "MEDIUM", col("transformation_count") * 1.5)
            .when(col("complexity_level") == "LOW", col("transformation_count") * 1)
            .otherwise(col("transformation_count") * 0.5)
        )
        
        logger.info("Transformation complexity assessment completed")
        return complexity_df
    
    def identify_deprecated_features(
        self,
        metadata_df: DataFrame
    ) -> DataFrame:
        """
        Identify deprecated PowerCenter features in use
        
        Args:
            metadata_df: DataFrame containing PowerCenter metadata
            
        Returns:
            DataFrame with deprecated features identified
        """
        logger.info("Identifying deprecated PowerCenter features")
        
        # Check for deprecated features
        deprecated_df = metadata_df.withColumn(
            "uses_deprecated_features",
            when(
                (col("component_type").isin(list(self.deprecated_features.keys()))) |
                (col("transformation_type").isin(list(self.deprecated_features.keys()))),
                True
            ).otherwise(False)
        ).withColumn(
            "deprecated_features_list",
            when(
                col("uses_deprecated_features") == True,
                array(col("component_type"))
            ).otherwise(array())
        )
        
        logger.info("Deprecated features identification completed")
        return deprecated_df
    
    def analyze_custom_code_dependencies(
        self,
        custom_code_df: DataFrame
    ) -> DataFrame:
        """
        Analyze custom code and third-party dependencies
        
        Args:
            custom_code_df: DataFrame containing custom code metadata
            
        Returns:
            DataFrame with dependency analysis
        """
        logger.info("Analyzing custom code and dependencies")
        
        dependency_df = custom_code_df.withColumn(
            "has_external_libraries",
            when(
                col("code_content").rlike("import|require|include|using"),
                True
            ).otherwise(False)
        ).withColumn(
            "has_stored_procedures",
            when(
                col("code_content").rlike("EXEC|EXECUTE|CALL|PROCEDURE"),
                True
            ).otherwise(False)
        ).withColumn(
            "has_dynamic_sql",
            when(
                col("code_content").rlike("EXECUTE IMMEDIATE|exec\\s*\\(|sp_executesql"),
                True
            ).otherwise(False)
        ).withColumn(
            "complexity_indicators",
            array(
                when(col("has_external_libraries") == True, lit("External Libraries")),
                when(col("has_stored_procedures") == True, lit("Stored Procedures")),
                when(col("has_dynamic_sql") == True, lit("Dynamic SQL")),
                when(length(col("code_content")) > 10000, lit("Large Code Block"))
            )
        ).withColumn(
            "migration_recommendation",
            when(
                col("has_stored_procedures") == True,
                lit("Convert stored procedures to PySpark UDFs or separate SQL scripts")
            )
            .when(
                col("has_dynamic_sql") == True,
                lit("Rewrite dynamic SQL using PySpark DataFrame API")
            )
            .when(
                col("has_external_libraries") == True,
                lit("Identify PySpark equivalents or package as custom libraries")
            )
            .otherwise(lit("Direct conversion possible with standard PySpark"))
        )
        
        logger.info("Custom code dependency analysis completed")
        return dependency_df
    
    def evaluate_data_quality_requirements(
        self,
        data_profiling_df: DataFrame
    ) -> DataFrame:
        """
        Evaluate data quality and validation requirements
        
        Args:
            data_profiling_df: DataFrame containing data profiling results
            
        Returns:
            DataFrame with data quality assessment
        """
        logger.info("Evaluating data quality requirements")
        
        dq_df = data_profiling_df.withColumn(
            "null_percentage",
            (col("null_count") / col("total_count")) * 100
        ).withColumn(
            "duplicate_percentage",
            (col("duplicate_count") / col("total_count")) * 100
        ).withColumn(
            "data_quality_score",
            when(
                (col("null_percentage") > 20) | (col("duplicate_percentage") > 10),
                1
            )
            .when(
                (col("null_percentage") > 10) | (col("duplicate_percentage") > 5),
                2
            )
            .when(
                (col("null_percentage") > 5) | (col("duplicate_percentage") > 2),
                3
            )
            .when(
                (col("null_percentage") > 1) | (col("duplicate_percentage") > 1),
                4
            )
            .otherwise(5)
        ).withColumn(
            "data_quality_issues",
            array(
                when(col("null_percentage") > 10, lit("High null rate")),
                when(col("duplicate_percentage") > 5, lit("High duplicate rate")),
                when(col("data_type_mismatch") == True, lit("Data type inconsistencies")),
                when(col("constraint_violations") > 0, lit("Constraint violations"))
            )
        ).withColumn(
            "validation_requirements",
            array(
                when(col("null_percentage") > 5, lit("Implement null handling strategy")),
                when(col("duplicate_percentage") > 2, lit("Add deduplication logic")),
                when(col("has_referential_integrity") == True, lit("Maintain referential integrity checks")),
                when(col("requires_range_validation") == True, lit("Add range validation"))
            )
        ).withColumn(
            "recommended_validations",
            concat_ws(
                "; ",
                when(col("null_percentage") > 5, lit("NOT NULL checks")),
                when(col("duplicate_percentage") > 2, lit("Uniqueness constraints")),
                when(col("data_type_mismatch") == True, lit("Type validation")),
                when(col("constraint_violations") > 0, lit("Business rule validation"))
            )
        )
        
        logger.info("Data quality evaluation completed")
        return dq_df
    
    def identify_performance_bottlenecks(
        self,
        execution_metrics_df: DataFrame
    ) -> DataFrame:
        """
        Identify performance bottlenecks and optimization needs
        
        Args:
            execution_metrics_df: DataFrame containing execution metrics
            
        Returns:
            DataFrame with performance analysis
        """
        logger.info("Identifying performance bottlenecks")
        
        # Calculate performance metrics
        perf_df = execution_metrics_df.withColumn(
            "avg_execution_time_minutes",
            col("total_execution_seconds") / 60
        ).withColumn(
            "throughput_mb_per_second",
            col("processed_data_mb") / col("total_execution_seconds")
        ).withColumn(
            "performance_rating",
            when(col("avg_execution_time_minutes") > 120, "CRITICAL")
            .when(col("avg_execution_time_minutes") > 60, "HIGH")
            .when(col("avg_execution_time_minutes") > 30, "MEDIUM")
            .when(col("avg_execution_time_minutes") > 10, "LOW")
            .otherwise("ACCEPTABLE")
        ).withColumn(
            "performance_concerns",
            array(
                when(col("throughput_mb_per_second") < 10, lit("Low throughput")),
                when(col("memory_usage_gb") > 50, lit("High memory consumption")),
                when(col("cpu_utilization") > 80, lit("CPU bottleneck")),
                when(col("disk_io_wait_time") > 30, lit("I/O bottleneck")),
                when(col("network_latency_ms") > 100, lit("Network latency"))
            )
        ).withColumn(
            "optimization_recommendations",
            array(
                when(col("throughput_mb_per_second") < 10, lit("Increase parallelism and partitioning")),
                when(col("memory_usage_gb") > 50, lit("Optimize memory usage with broadcast joins")),
                when(col("cpu_utilization") > 80, lit("Review compute-intensive transformations")),
                when(col("disk_io_wait_time") > 30, lit("Optimize I/O with columnar formats")),
                when(col("has_cartesian_joins") == True, lit("Eliminate cartesian joins")),
                when(col("lookup_count") > 20, lit("Convert lookups to broadcast joins"))
            )
        ).withColumn(
            "spark_optimization_strategy",
            when(
                col("data_volume_gb") > 1000,
                lit("Use dynamic partition pruning, bucketing, and Z-ordering")
            )
            .when(
                col("lookup_count") > 10,
                lit("Use broadcast joins for small dimension tables")
            )
            .when(
                col("has_aggregations") == True,
                lit("Use partial aggregation and appropriate partition keys")
            )
            .otherwise(lit("Standard Spark optimizations sufficient"))
        )
        
        logger.info("Performance bottleneck identification completed")
        return perf_df
    
    def document_technical_debt(
        self,
        metadata_df: DataFrame,
        execution_history_df: DataFrame
    ) -> DataFrame:
        """
        Document technical debt and workarounds
        
        Args:
            metadata_df: DataFrame containing PowerCenter metadata
            execution_history_df: DataFrame containing execution history
            
        Returns:
            DataFrame with technical debt documentation
        """
        logger.info("Documenting technical debt and workarounds")
        
        # Identify technical debt indicators
        debt_df = metadata_df.alias("meta").join(
            execution_history_df.alias("exec"),
            col("meta.component_id") == col("exec.component_id"),
            "left"
        ).withColumn(
            "has_error_handling_workarounds",
            when(
                col("meta.error_handling_type") == "IGNORE_ERRORS",
                True
            ).otherwise(False)
        ).withColumn(
            "has_data_quality_workarounds",
            when(
                (col("exec.error_count") > 0) &
                (col("exec.status") == "SUCCESS"),
                True
            ).otherwise(False)
        ).withColumn(
            "has_performance_workarounds",
            when(
                col("meta.component_notes").rlike("workaround|hack|temporary|todo|fixme"),
                True
            ).otherwise(False)
        ).withColumn(
            "technical_debt_score",
            (
                when(col("has_error_handling_workarounds") == True, 3).otherwise(0) +
                when(col("has_data_quality_workarounds") == True, 3).otherwise(0) +
                when(col("has_performance_workarounds") == True, 2).otherwise(0) +
                when(col("uses_deprecated_features") == True, 4).otherwise(0) +
                when(col("custom_code_lines") > 1000, 2).otherwise(0)
            )
        ).withColumn(
            "technical_debt_level",
            when(col("technical_debt_score") >= 10, "CRITICAL")
            .when(col("technical_debt_score") >= 7, "HIGH")
            .when(col("technical_debt_score") >= 4, "MEDIUM")
            .when(col("technical_debt_score") >= 1, "LOW")
            .otherwise("NONE")
        ).withColumn(
            "refactoring_recommendations",
            array(
                when(
                    col("has_error_handling_workarounds") == True,
                    lit("Implement proper error handling with try-except blocks")
                ),
                when(
                    col("has_data_quality_workarounds") == True,
                    lit("Add comprehensive data quality checks")
                ),
                when(
                    col("uses_deprecated_features") == True,
                    lit("Replace deprecated features with modern alternatives")
                ),
                when(
                    col("custom_code_lines") > 1000,
                    lit("Refactor large code blocks into modular functions")
                )
            )
        ).withColumn(
            "migration_impact",
            when(
                col("technical_debt_level") == "CRITICAL",
                lit("Requires significant refactoring before migration")
            )
            .when(
                col("technical_debt_level") == "HIGH",
                lit("Plan additional time for cleanup during migration")
            )
            .when(
                col("technical_debt_level") == "MEDIUM",
                lit("Address during migration with improved patterns")
            )
            .otherwise(lit("Minimal impact on migration"))
        )
        
        logger.info("Technical debt documentation completed")
        return debt_df
    
    def create_risk_register(
        self,
        complexity_df: DataFrame,
        dq_df: DataFrame,
        perf_df: DataFrame,
        debt_df: DataFrame
    ) -> DataFrame:
        """
        Create comprehensive risk register with severity ratings
        
        Args:
            complexity_df: Complexity analysis DataFrame
            dq_df: Data quality analysis DataFrame
            perf_df: Performance analysis DataFrame
            debt_df: Technical debt analysis DataFrame
            
        Returns:
            DataFrame containing risk register
        """
        logger.info("Creating risk register")
        
        # Aggregate all risk factors
        risk_components = complexity_df.alias("comp") \
            .join(dq_df.alias("dq"), col("comp.component_id") == col("dq.component_id"), "left") \
            .join(perf_df.alias("perf"), col("comp.component_id") == col("perf.component_id"), "left") \
            .join(debt_df.alias("debt"), col("comp.component_id") == col("debt.component_id"), "left")
        
        # Generate risk items
        risk_register = risk_components.select(
            concat(lit("RISK-"), col("comp.component_id")).alias("risk_id"),
            lit("TECHNICAL").alias("category"),
            when(
                col("comp.complexity_level") == "VERY_HIGH",
                lit("High Complexity Migration")
            )
            .when(
                col("dq.data_quality_score") < 3,
                lit("Data Quality")
            )
            .when(
                col("perf.performance_rating") == "CRITICAL",
                lit("Performance")
            )
            .when(
                col("debt.technical_debt_level").isin("CRITICAL", "HIGH"),
                lit("Technical Debt")
            )
            .otherwise(lit("General"))
            .alias("subcategory"),
            concat_ws(
                " | ",
                when(
                    col("comp.complexity_level") == "VERY_HIGH",
                    concat(
                        lit("Complex transformation requiring "),
                        col("comp.estimated_effort_days"),
                        lit(" days effort")
                    )
                ),
                when(
                    col("dq.data_quality_score") < 3,
                    concat(
                        lit("Data quality issues: "),
                        concat_ws(", ", col("dq.data_quality_issues"))
                    )
                ),
                when(
                    col("perf.performance_rating") == "CRITICAL",
                    concat(
                        lit("Performance bottleneck: "),
                        concat_ws(", ", col("perf.performance_concerns"))
                    )
                ),
                when(
                    col("debt.technical_debt_level").isin("CRITICAL", "HIGH"),
                    lit("Significant technical debt requiring refactoring")
                )
            ).alias("description"),
            when(
                (col("comp.complexity_level") == "VERY_HIGH") &
                (col("debt.technical_debt_level") == "CRITICAL"),
                lit("CRITICAL")
            )
            .when(
                (col("comp.complexity_level") == "VERY_HIGH") |
                (col("perf.performance_rating") == "CRITICAL") |
                (col("debt.technical_debt_level") == "CRITICAL"),
                lit("HIGH")
            )
            .when(
                (col("comp.complexity_level") == "HIGH") |
                (col("dq.data_quality_score") < 3) |
                (col("debt.technical_debt_level") == "HIGH"),
                lit("MEDIUM")
            )
            .when(
                col("comp.complexity_level") == "MEDIUM",
                lit("LOW")
            )
            .otherwise(lit("INFORMATIONAL"))
            .alias("severity"),
            when(col("severity") == "CRITICAL", 5)
            .when(col("severity") == "HIGH", 4)
            .when(col("severity") == "MEDIUM", 3)
            .when(col("severity") == "LOW", 2)
            .otherwise(1)
            .alias("impact_score"),
            when(
                col("comp.uses_deprecated_features") == True,
                5
            )
            .when(
                col("comp.overall_complexity_score") > 4,
                4
            )
            .when(
                col("comp.overall_complexity_score") > 3,
                3
            )
            .otherwise(2)
            .alias("probability_score"),
            (col("impact_score") * col("probability_score")).alias("risk_score"),
            array(col("comp.component_name")).alias("affected_components"),
            when(
                col("risk_score") >= 15,
                lit("PLANNING")
            )
            .when(
                col("risk_score") >= 10,
                lit("DEVELOPMENT")
            )
            .otherwise(lit("TESTING"))
            .alias("affected_phase"),
            when(
                col("comp.uses_deprecated_features") == True,
                lit("Usage of deprecated PowerCenter features")
            )
            .when(
                col("comp.complexity_level") == "VERY_HIGH",
                lit("Complex business logic and transformations")
            )
            .when(
                col("dq.data_quality_score") < 3,
                lit("Underlying data quality issues")
            )
            .otherwise(lit("Standard migration challenges"))
            .alias("root_cause"),
            concat_ws(
                " | ",
                col("comp.recommended_approach"),
                concat_ws("; ", col("dq.recommended_validations")),
                concat_ws("; ", col("perf.optimization_recommendations")),
                concat_ws("; ", col("debt.refactoring_recommendations"))
            ).alias("mitigation_strategy"),
            when(
                col("severity") == "CRITICAL",
                lit("Allocate senior resources; Consider phased migration; Implement fallback mechanisms")
            )
            .when(
                col("severity") == "HIGH",
                lit("Parallel run with legacy system; Comprehensive testing; Rollback plan")
            )
            .otherwise(lit("Standard rollback procedures; Additional