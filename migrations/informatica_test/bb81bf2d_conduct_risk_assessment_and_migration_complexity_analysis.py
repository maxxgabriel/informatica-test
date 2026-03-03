from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InformaticaMigrationRiskAssessment:
    """
    Comprehensive Risk Assessment and Migration Complexity Analysis Framework
    for Informatica PowerCenter to PySpark Migration
    """
    
    def __init__(self, spark_session=None):
        """Initialize Risk Assessment Framework"""
        self.spark = spark_session or SparkSession.builder \
            .appName("InformaticaMigrationRiskAssessment") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.assessment_timestamp = datetime.now()
        logger.info("Risk Assessment Framework initialized")
    
    def create_risk_register_schema(self):
        """Define comprehensive risk register schema"""
        return StructType([
            StructField("risk_id", StringType(), False),
            StructField("category", StringType(), False),
            StructField("subcategory", StringType(), True),
            StructField("risk_description", StringType(), False),
            StructField("source_component", StringType(), True),
            StructField("severity", StringType(), False),
            StructField("probability", StringType(), False),
            StructField("impact", StringType(), False),
            StructField("risk_score", IntegerType(), False),
            StructField("mitigation_strategy", StringType(), False),
            StructField("contingency_plan", StringType(), True),
            StructField("owner", StringType(), False),
            StructField("status", StringType(), False),
            StructField("target_resolution_date", DateType(), True),
            StructField("dependencies", ArrayType(StringType()), True),
            StructField("technical_debt", BooleanType(), False),
            StructField("blocking_issue", BooleanType(), False),
            StructField("assessment_date", TimestampType(), False),
            StructField("last_updated", TimestampType(), False),
            StructField("notes", StringType(), True)
        ])
    
    def assess_transformation_complexity(self, metadata_df):
        """
        Assess complexity of Informatica transformations and business logic
        
        Args:
            metadata_df: DataFrame containing PowerCenter metadata
            
        Returns:
            DataFrame with complexity scores and risk indicators
        """
        complexity_assessment = metadata_df.select(
            col("object_name"),
            col("object_type"),
            col("transformation_type"),
            col("expression_count"),
            col("lookup_count"),
            col("join_complexity"),
            col("aggregation_count"),
            col("custom_sql_count")
        ).withColumn(
            "complexity_score",
            when(col("transformation_type").isin(
                "Expression", "Filter", "Router"), 1)
            .when(col("transformation_type").isin(
                "Lookup", "Joiner"), 2)
            .when(col("transformation_type").isin(
                "Aggregator", "Sorter"), 3)
            .when(col("transformation_type").isin(
                "Custom", "Java", "SQL"), 4)
            .otherwise(2)
        ).withColumn(
            "expression_complexity",
            when(col("expression_count") < 10, 1)
            .when(col("expression_count").between(10, 50), 2)
            .when(col("expression_count").between(51, 100), 3)
            .when(col("expression_count") > 100, 4)
            .otherwise(0)
        ).withColumn(
            "total_complexity_score",
            col("complexity_score") + 
            col("expression_complexity") + 
            coalesce(col("lookup_count"), lit(0)) + 
            coalesce(col("aggregation_count"), lit(0))
        ).withColumn(
            "complexity_rating",
            when(col("total_complexity_score") <= 5, "LOW")
            .when(col("total_complexity_score").between(6, 10), "MEDIUM")
            .when(col("total_complexity_score").between(11, 15), "HIGH")
            .when(col("total_complexity_score") > 15, "CRITICAL")
            .otherwise("UNKNOWN")
        ).withColumn(
            "migration_effort_days",
            when(col("complexity_rating") == "LOW", lit(2))
            .when(col("complexity_rating") == "MEDIUM", lit(5))
            .when(col("complexity_rating") == "HIGH", lit(10))
            .when(col("complexity_rating") == "CRITICAL", lit(20))
            .otherwise(lit(5))
        )
        
        logger.info("Transformation complexity assessment completed")
        return complexity_assessment
    
    def identify_deprecated_features(self, metadata_df):
        """
        Identify deprecated PowerCenter features and incompatible components
        
        Args:
            metadata_df: DataFrame containing PowerCenter components
            
        Returns:
            DataFrame with deprecated features and migration risks
        """
        deprecated_features = [
            "PowerMart", "PowerCenter Repository Service",
            "Command Task", "Email Task", "Event-Wait Task",
            "HTTP Transformation", "External Procedure",
            "PowerCenter Connect", "B2B Data Transformation",
            "Legacy Lookup", "Unconnected Lookup"
        ]
        
        deprecated_df = metadata_df.filter(
            col("object_type").isin(deprecated_features) |
            col("version").rlike("(?i)(7\\.|8\\.|9\\.[0-5])") |
            col("features").rlike("(?i)(pmcmd|pmrep|infacmd)") |
            col("transformation_type").isin(
                "External Procedure", "Java Transformation", "Custom"
            )
        ).withColumn(
            "deprecation_risk",
            when(col("object_type").isin(
                "PowerMart", "External Procedure"), "CRITICAL")
            .when(col("transformation_type") == "Java Transformation", "HIGH")
            .when(col("version").rlike("(?i)(7\\.|8\\.)"), "HIGH")
            .when(col("version").rlike("(?i)(9\\.[0-5])"), "MEDIUM")
            .otherwise("LOW")
        ).withColumn(
            "migration_action",
            when(col("deprecation_risk") == "CRITICAL", 
                 "REQUIRES_CUSTOM_DEVELOPMENT")
            .when(col("deprecation_risk") == "HIGH", 
                  "REDESIGN_REQUIRED")
            .when(col("deprecation_risk") == "MEDIUM", 
                  "MODIFICATION_NEEDED")
            .otherwise("STANDARD_MIGRATION")
        ).withColumn(
            "recommended_pyspark_alternative",
            when(col("transformation_type") == "Java Transformation",
                 "Custom Python UDF or PySpark Native Functions")
            .when(col("transformation_type") == "External Procedure",
                 "Python subprocess or external API call")
            .when(col("transformation_type") == "Lookup",
                 "broadcast join or DataFrame join")
            .when(col("object_type") == "Command Task",
                 "Python subprocess or shell commands")
            .otherwise("Standard PySpark Transformations")
        )
        
        logger.info(f"Identified {deprecated_df.count()} deprecated features")
        return deprecated_df
    
    def document_custom_code_dependencies(self, code_inventory_df):
        """
        Document custom code, scripts, and third-party dependencies
        
        Args:
            code_inventory_df: DataFrame with custom code inventory
            
        Returns:
            DataFrame with dependency analysis
        """
        dependency_analysis = code_inventory_df.withColumn(
            "code_type",
            when(col("file_extension") == ".java", "JAVA")
            .when(col("file_extension") == ".sql", "SQL")
            .when(col("file_extension") == ".sh", "SHELL")
            .when(col("file_extension") == ".jar", "JAVA_LIBRARY")
            .otherwise("OTHER")
        ).withColumn(
            "migration_complexity",
            when(col("code_type") == "JAVA", "HIGH")
            .when(col("code_type") == "SHELL", "MEDIUM")
            .when(col("code_type") == "SQL", "LOW")
            .otherwise("UNKNOWN")
        ).withColumn(
            "dependency_count",
            size(split(col("import_statements"), ","))
        ).withColumn(
            "external_libraries",
            when(col("code_type") == "JAVA",
                 array_distinct(
                     filter(
                         split(col("import_statements"), ","),
                         lambda x: ~x.rlike("(?i)(java\\.lang|java\\.util)")
                     )
                 ))
            .otherwise(array())
        ).withColumn(
            "migration_effort",
            when(col("lines_of_code") < 100, "LOW")
            .when(col("lines_of_code").between(100, 500), "MEDIUM")
            .when(col("lines_of_code").between(501, 1000), "HIGH")
            .when(col("lines_of_code") > 1000, "CRITICAL")
            .otherwise("UNKNOWN")
        ).withColumn(
            "requires_rewrite",
            when(col("code_type") == "JAVA", lit(True))
            .when(col("dependency_count") > 10, lit(True))
            .when(size(col("external_libraries")) > 5, lit(True))
            .otherwise(lit(False))
        )
        
        logger.info("Custom code and dependency analysis completed")
        return dependency_analysis
    
    def evaluate_data_quality_requirements(self, data_profile_df):
        """
        Evaluate data quality and validation requirements
        
        Args:
            data_profile_df: DataFrame with data profiling results
            
        Returns:
            DataFrame with data quality assessment
        """
        data_quality_assessment = data_profile_df.withColumn(
            "null_percentage",
            round((col("null_count") / col("total_records")) * 100, 2)
        ).withColumn(
            "duplicate_percentage",
            round((col("duplicate_count") / col("total_records")) * 100, 2)
        ).withColumn(
            "data_quality_score",
            (lit(100) - 
             col("null_percentage") - 
             col("duplicate_percentage") - 
             col("invalid_format_percentage"))
        ).withColumn(
            "quality_rating",
            when(col("data_quality_score") >= 95, "EXCELLENT")
            .when(col("data_quality_score").between(85, 94), "GOOD")
            .when(col("data_quality_score").between(70, 84), "FAIR")
            .when(col("data_quality_score").between(50, 69), "POOR")
            .when(col("data_quality_score") < 50, "CRITICAL")
            .otherwise("UNKNOWN")
        ).withColumn(
            "validation_rules_required",
            array_distinct(
                array(
                    when(col("null_percentage") > 5, 
                         lit("NOT_NULL_CONSTRAINT")).otherwise(lit(None)),
                    when(col("duplicate_percentage") > 2,
                         lit("UNIQUE_KEY_VALIDATION")).otherwise(lit(None)),
                    when(col("invalid_format_percentage") > 1,
                         lit("FORMAT_VALIDATION")).otherwise(lit(None)),
                    when(col("outlier_count") > 0,
                         lit("RANGE_VALIDATION")).otherwise(lit(None))
                )
            )
        ).withColumn(
            "cleansing_required",
            when(col("quality_rating").isin("POOR", "CRITICAL"), lit(True))
            .otherwise(lit(False))
        ).withColumn(
            "migration_risk",
            when(col("quality_rating") == "CRITICAL", "HIGH")
            .when(col("quality_rating") == "POOR", "MEDIUM")
            .otherwise("LOW")
        )
        
        logger.info("Data quality assessment completed")
        return data_quality_assessment
    
    def identify_performance_bottlenecks(self, performance_metrics_df):
        """
        Identify performance bottlenecks and optimization needs
        
        Args:
            performance_metrics_df: DataFrame with current performance metrics
            
        Returns:
            DataFrame with performance analysis and recommendations
        """
        performance_analysis = performance_metrics_df.withColumn(
            "avg_runtime_minutes",
            col("avg_runtime_seconds") / 60
        ).withColumn(
            "performance_rating",
            when(col("avg_runtime_minutes") < 15, "EXCELLENT")
            .when(col("avg_runtime_minutes").between(15, 60), "GOOD")
            .when(col("avg_runtime_minutes").between(61, 180), "FAIR")
            .when(col("avg_runtime_minutes") > 180, "POOR")
            .otherwise("UNKNOWN")
        ).withColumn(
            "bottleneck_indicators",
            array_distinct(
                array(
                    when(col("lookup_cache_size_mb") > 1024,
                         lit("LARGE_LOOKUP_CACHE")).otherwise(lit(None)),
                    when(col("sort_buffer_size_mb") > 512,
                         lit("EXCESSIVE_SORTING")).otherwise(lit(None)),
                    when(col("aggregation_rows") > 10000000,
                         lit("HIGH_CARDINALITY_AGGREGATION")).otherwise(lit(None)),
                    when(col("source_query_time_sec") > 300,
                         lit("SLOW_SOURCE_QUERY")).otherwise(lit(None)),
                    when(col("target_commit_time_sec") > 600,
                         lit("SLOW_TARGET_LOAD")).otherwise(lit(None))
                )
            )
        ).withColumn(
            "optimization_recommendations",
            array_distinct(
                array(
                    when(col("lookup_cache_size_mb") > 1024,
                         lit("Convert to broadcast join or repartition"))
                    .otherwise(lit(None)),
                    when(col("sort_buffer_size_mb") > 512,
                         lit("Use sortWithinPartitions or avoid unnecessary sorting"))
                    .otherwise(lit(None)),
                    when(col("aggregation_rows") > 10000000,
                         lit("Pre-aggregate or use two-stage aggregation"))
                    .otherwise(lit(None)),
                    when(col("source_query_time_sec") > 300,
                         lit("Optimize source query with pushdown predicates"))
                    .otherwise(lit(None)),
                    when(col("target_commit_time_sec") > 600,
                         lit("Use bulk inserts or optimize target partitioning"))
                    .otherwise(lit(None))
                )
            )
        ).withColumn(
            "migration_priority",
            when(col("performance_rating") == "POOR", "HIGH")
            .when(col("performance_rating") == "FAIR", "MEDIUM")
            .otherwise("LOW")
        ).withColumn(
            "expected_pyspark_improvement",
            when(size(col("bottleneck_indicators")) >= 3, "50-70%")
            .when(size(col("bottleneck_indicators")) == 2, "30-50%")
            .when(size(col("bottleneck_indicators")) == 1, "20-30%")
            .otherwise("10-20%")
        )
        
        logger.info("Performance bottleneck analysis completed")
        return performance_analysis
    
    def document_technical_debt(self, metadata_df, issue_tracking_df):
        """
        Document technical debt and workarounds in current implementation
        
        Args:
            metadata_df: DataFrame with component metadata
            issue_tracking_df: DataFrame with known issues and workarounds
            
        Returns:
            DataFrame with technical debt inventory
        """
        technical_debt = metadata_df.alias("m").join(
            issue_tracking_df.alias("i"),
            col("m.object_name") == col("i.component_name"),
            "left"
        ).select(
            col("m.object_name"),
            col("m.object_type"),
            col("m.last_modified_date"),
            col("m.modification_count"),
            col("i.issue_type"),
            col("i.workaround_description"),
            col("i.original_design_intent"),
            col("i.current_implementation")
        ).withColumn(
            "debt_category",
            when(col("issue_type") == "DESIGN_FLAW", "ARCHITECTURAL")
            .when(col("issue_type") == "PERFORMANCE_HACK", "PERFORMANCE")
            .when(col("issue_type") == "DATA_QUALITY_BYPASS", "DATA_QUALITY")
            .when(col("issue_type") == "ERROR_HANDLING_SKIP", "ERROR_HANDLING")
            .otherwise("OTHER")
        ).withColumn(
            "debt_severity",
            when(col("modification_count") > 20, "HIGH")
            .when(col("modification_count").between(10, 20), "MEDIUM")
            .when(col("modification_count") < 10, "LOW")
            .otherwise("UNKNOWN")
        ).withColumn(
            "refactoring_opportunity",
            when(col("debt_category") == "ARCHITECTURAL",
                 "Redesign with modern PySpark patterns")
            .when(col("debt_category") == "PERFORMANCE",
                 "Implement proper optimization techniques")
            .when(col("debt_category") == "DATA_QUALITY",
                 "Add comprehensive data validation")
            .when(col("debt_category") == "ERROR_HANDLING",
                 "Implement robust exception handling")
            .otherwise("Review and refactor as needed")
        ).withColumn(
            "clean_slate_benefit",
            when(col("debt_severity") == "HIGH", "SIGNIFICANT")
            .when(col("debt_severity") == "MEDIUM", "MODERATE")
            .otherwise("MINOR")
        )
        
        logger.info("Technical debt documentation completed")
        return technical_debt
    
    def create_comprehensive_risk_register(self, assessment_components):
        """
        Create comprehensive risk register from all assessment components
        
        Args:
            assessment_components: Dictionary of assessment DataFrames
            
        Returns:
            DataFrame with complete risk register
        """
        risk_register_data = []
        
        # Transformation Complexity Risks
        complexity_risks = assessment_components['complexity'].filter(
            col("complexity_rating").isin("HIGH", "CRITICAL")
        ).select(
            concat(lit("COMP-"), monotonically_increasing_id()).alias("risk_id"),
            lit("TRANSFORMATION_COMPLEXITY").alias("category"),
            col("transformation_type").alias("subcategory"),
            concat(
                lit("Complex transformation: "),
                col("object_name"),
                lit(" with complexity score: "),
                col("total_complexity_score")
            ).alias("risk_description"),
            col("object_name").alias("source_component"),
            col("complexity_rating").alias("severity"),
            lit("MEDIUM").alias("probability"),
            when(col("complexity_rating") == "CRITICAL", "HIGH")
            .otherwise("MEDIUM").alias("impact"),
            when(col("complexity_rating") == "CRITICAL", lit(9))
            .when(col("complexity_rating") == "HIGH", lit(6))
            .otherwise(lit(4)).alias("risk_score"),
            lit("Break down into smaller components; conduct proof-of-concept; allocate senior resources").alias("mitigation_strategy"),
            lit("Extend timeline; engage SMEs; consider phased migration").alias("contingency_plan"),
            lit("Migration Lead").alias("owner"),
            lit("IDENTIFIED").alias("status"),
            date_add(current_date(), 30).alias("target_resolution_date"),
            array(lit("RESOURCE_AVAILABILITY"), lit("TECHNICAL_EXPERTISE")).alias("dependencies"),
            lit(False).alias("technical_debt"),
            when(col("complexity_rating") == "CRITICAL", lit(True))
            .otherwise(lit(False)).alias("blocking_issue"),
            current_timestamp().alias("assessment_date"),
            current_timestamp().alias("last_updated"),
            lit(None).cast("string").alias("notes")
        )
        
        # Deprecated Features Risks
        deprecated_risks = assessment_components['deprecated'].select(
            concat(lit("DEPR-"), monotonically_increasing_id()).alias("risk_id"),
            lit("DEPRECATED_FEATURE").alias("category"),
            col("object_type").alias("subcategory"),
            concat(
                lit("Deprecated feature: "),
                col("object_type"),
                lit(" requires "),
                col("migration_action")
            ).alias("risk_description"),
            col("object_name").alias("source_component"),
            col("deprecation_risk").alias("severity"),
            lit("HIGH").alias("probability"),
            col("deprecation_risk").alias("impact"),
            when(col("deprecation_risk") == "CRITICAL", lit(9))
            .when(col("deprecation_risk") == "HIGH", lit(6))
            .otherwise(lit(4)).alias("risk_score"),
            concat(
                lit("Implement "),
                col("recommended_pyspark_alternative"),
                lit("; conduct thorough testing")
            ).alias("mitigation_strategy"),
            lit("Develop custom solution; allocate additional development time").alias("contingency_plan"),
            lit("Technical Architect").alias("owner"),
            lit("IDENTIFIED").alias("status"),
            date_add(current_date(), 45).alias("target_resolution_date"),
            array(lit("CUSTOM_DEVELOPMENT"), lit("TESTING")).alias("dependencies"),
            lit(False).alias("technical_debt"),
            when(col("deprecation_risk") == "CRITICAL", lit(True))
            .otherwise(lit(False)).alias("blocking_issue"),
            current_timestamp().alias("assessment_date"),
            current_timestamp().alias("last_updated"),
            lit(None).cast("string").alias("notes")
        )
        
        # Data Quality Risks
        data_quality_risks = assessment_components['data_quality'].filter(
            col("quality_rating").isin("POOR", "CRITICAL")
        ).select(
            concat(lit("DQ-"), monotonically_increasing_id()).alias("risk_id"),
            lit("DATA_QUALITY").alias("category"),
            lit("VALIDATION").alias("subcategory"),
            concat(
                lit("Poor data quality in "),
                col("table_name"),
                lit(" with score: "),
                col("data_quality_score")
            ).alias("risk_description"),
            col("table_name").alias("source_component"),
            when(col("quality_rating") == "CRITICAL", "HIGH")
            .otherwise("MEDIUM").alias("severity"),
            lit("HIGH").alias("probability"),
            lit("HIGH").alias("impact"),
            lit(6).alias("risk_score"),
            lit("Implement data cleansing; add validation rules; establish data quality framework").alias("mitigation_strategy"),
            lit("Manual data correction; phased data migration; enhanced monitoring").alias("contingency_plan"),
            lit("Data Quality Lead").alias("owner"),
            lit("IDENTIFIED").alias("status"),
            date_add(current_date(), 60).alias("target_resolution_date"),
            array(lit("DATA_CLEANSING"), lit("BUSINESS_VALIDATION")).alias("dependencies"),
            lit(False).alias("technical_debt"),
            lit(False).alias("blocking_issue"),
            current_timestamp().alias("assessment_date"),
            current_timestamp().alias("last_updated"),
            lit(None).cast("string").alias("notes")
        )
        
        # Performance Risks
        performance_risks = assessment_components['performance'].filter(
            col("performance_rating").isin("FAIR", "POOR")
        ).select(
            concat(lit("PERF-"), monotonically_increasing_id()).alias("risk_id"),
            lit("PERFORMANCE").alias("category"),
            lit("OPTIMIZATION").alias("subcategory"),
            concat(
                lit("Performance bottleneck in "),
                col("workflow_name"),
                lit(" with runtime: "),
                col("avg_runtime_minutes"),
                lit(" minutes")
            ).alias("risk_description"),
            col("workflow_name").alias("source_component"),
            when(col("performance_rating") == "POOR", "HIGH")
            .otherwise("MEDIUM").alias("severity"),
            lit("MEDIUM").alias("probability"),
            lit("MEDIUM").alias("impact"),
            when(col("performance_rating") == "POOR", lit(6))
            .otherwise(lit(4)).alias("risk_score"),
            concat(
                lit("Apply optimization recommendations: "),
                array_join(col("optimization_recommendations"), "; ")
            ).alias("mitigation_strategy"),
            lit("Performance testing; incremental optimization; resource scaling").alias("contingency_plan"),
            lit("Performance Engineer").alias("owner"),
            lit("IDENTIFIED").alias("status"),
            date_add(current_date(), 30).alias("target_resolution_date"),
            array(lit("INFRASTRUCTURE"), lit("PERFORMANCE_TESTING")).alias("dependencies"),
            lit(False).alias("technical_debt"),
            lit(False).alias("blocking_issue"),
            current_timestamp().alias("assessment_date"),
            current_timestamp().alias("last_updated"),
            lit(None).cast("string").alias("notes")
        )
        
        # Technical Debt Risks
        tech_debt_risks = assessment_components['technical_debt'].filter(
            col("debt_severity") == "HIGH"
        ).select(
            concat(lit("DEBT-"), monotonically_increasing_id()).alias("risk_id"),
            lit("TECHNICAL_DEBT").alias("category"),
            col("debt_category").alias("subcategory"),
            concat(
                lit("Technical debt in "),
                col("object_name"),
                lit(": "),
                col("workaround_description")
            ).alias("risk_description"),
            col("object_name").alias("source_component"),
            col("debt_severity").alias("severity"),
            lit("MEDIUM").alias("probability"),
            lit("MEDIUM").alias("impact"),
            lit(4).alias("risk_score"),
            col("refactoring_opportunity").alias("mitigation_strategy"),
            lit("Address during migration; allocate refactoring time; document properly").alias("contingency_plan"),
            lit("Development Lead").alias("owner"),
            lit("IDENTIFIED").alias("status"),
            date_add(current_date(), 45).alias("target_resolution_date"),
            array(lit("REFACTORING"), lit("CODE_REVIEW")).alias("dependencies"),
            lit(True).alias("technical_debt"),
            lit(False).alias("blocking_issue"),
            current_timestamp().alias("assessment_date"),
            current_timestamp().alias("last_updated"),
            lit(None).cast("string").alias("notes")
        )
        
        # Combine all risk categories
        comprehensive_risk_register = complexity_risks \
            .union(deprecated_risks) \
            .union(data_quality_risks) \
            .union(performance_risks) \
            .union(tech_debt_risks)
        
        logger.info(f"Comprehensive risk register created with {comprehensive_risk_register.count()} risks")
        return comprehensive_risk_register
    
    def calculate_risk_metrics(self, risk_register_df):
        """
        Calculate risk metrics and aggregate statistics
        
        Args:
            risk_register_df: Comprehensive risk register DataFrame
            
        Returns:
            DataFrame with risk metrics and KPIs
        """
        risk_metrics = risk_register_df.groupBy("category").agg(
            count("*").alias("total_risks"),
            sum(when(col("severity") == "CRITICAL", 1).otherwise(0)).alias("critical_risks"),
            sum(when(col("severity") == "HIGH", 1).otherwise(0)).alias("high_risks"),
            sum(when(col("severity") == "MEDIUM", 1).otherwise(0)).alias("medium_risks"),
            sum(when(col("severity") == "LOW", 1).otherwise(0)).alias("low_risks"),
            sum(when(col("blocking_issue") == True, 1).otherwise(0)).alias("blocking_issues"),
            avg("risk_score").alias("avg_risk_score"),
            max("risk_score").alias("max_risk_score")
        ).withColumn(
            "risk_distribution",
            struct(
                col("critical_risks"),
                col("high_risks"),
                col("medium_risks"),
                col("low_risks")
            )
        ).withColumn(
            "category_priority",
            when(col("critical_risks") > 0, "CRITICAL")
            .when(col("blocking_issues") > 0, "CRITICAL")
            .when(col("high_risks") > 3, "HIGH")
            .when(col("avg_risk_score") > 6, "HIGH")
            .otherwise("MEDIUM")
        )
        
        overall_metrics = risk_register_df.agg(
            count("*").alias("total_risks"),
            countDistinct("category").alias("risk_categories"),
            sum(when(col("blocking_issue") == True, 1).otherwise(0)).alias("total_blockers"),
            avg("risk_score").alias("overall_risk_score"),
            sum(when(col("severity").isin("CRITICAL", "HIGH"), 1).otherwise(0)).alias("high_severity_count")
        ).withColumn(
            "overall_risk_level",
            when(col("total_blockers") > 0, "CRITICAL")
            .when(col("overall_risk_score") > 7, "HIGH")
            .when(col("overall_risk_score") > 5, "MEDIUM")
            .otherwise("LOW")
        ).withColumn(
            "migration_readiness",
            when(col("overall_risk_level") == "CRITICAL", "NOT_READY")
            .when(col("overall_risk_level") == "HIGH", "NEEDS_PREPARATION")
            .when(col("overall_risk_level") == "MEDIUM", "READY_WITH_PLANNING")
            .otherwise("READY")
        )
        
        logger.info("Risk metrics calculated")
        return {"category_metrics": risk_metrics, "overall_metrics": overall_metrics}
    
    def establish_go_nogo_criteria(self, risk_metrics):
        """
        Establish go/no-go decision criteria based on risk assessment
        
        Args:
            risk_metrics: Dictionary with risk metrics DataFrames
            
        Returns:
            DataFrame with go/no-go decision framework
        """
        overall_metrics = risk_metrics['overall_metrics']
        
        decision_criteria = overall_metrics.withColumn(
            "criteria_results",
            struct(
                when(col("total_blockers") == 0, True).otherwise(False).alias("no_blocking_issues"),
                when(col("overall_risk_score") <= 6, True).otherwise(False).alias("acceptable_risk_level"),
                when(col("high_severity_count") <= 5, True).otherwise(False).alias("manageable_high_risks"),
                lit(True).alias("stakeholder_approval"),
                lit(True).alias("resource_availability"),
                lit(True).alias("timeline_feasibility")
            )
        ).withColumn(
            "go_decision",
            when(
                col("criteria_results.no_blocking_issues") &
                col("criteria_results.acceptable_risk_level") &
                col("criteria_results.manageable_high_risks"),
                "GO"
            ).when(
                col("criteria_results.no_blocking_issues") &
                (col("overall_risk_score") <= 7),
                "GO_WITH_CONDITIONS"
            ).otherwise("NO_GO")
        ).withColumn(
            "conditions_for_go",
            when(col("go_decision") == "GO_WITH_CONDITIONS",
                 array(
                     lit("Complete high-risk mitigation plans"),
                     lit("Allocate additional resources