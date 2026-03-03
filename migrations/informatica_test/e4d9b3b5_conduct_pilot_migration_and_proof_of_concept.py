# pilot_migration_poc.py
# Pilot Migration and Proof of Concept Framework
# Validates migration approach from Informatica PowerCenter to PySpark

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging
import json
import hashlib
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ValidationStatus(Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"


@dataclass
class ValidationResult:
    validation_type: str
    status: ValidationStatus
    source_count: int
    target_count: int
    match_percentage: float
    discrepancies: List[Dict[str, Any]]
    execution_time_seconds: float
    timestamp: str


@dataclass
class PerformanceMetrics:
    workflow_name: str
    execution_time_seconds: float
    records_processed: int
    records_per_second: float
    memory_usage_mb: float
    cpu_usage_percent: float
    baseline_time_seconds: float
    performance_improvement_percent: float


@dataclass
class LessonLearned:
    category: str
    description: str
    impact: str
    recommendation: str
    priority: str


class PilotMigrationFramework:
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.validation_results = []
        self.performance_metrics = []
        self.lessons_learned = []
        
    def create_spark_session(self) -> SparkSession:
        return SparkSession.builder \
            .appName("Pilot_Migration_POC") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .enableHiveSupport() \
            .getOrCreate()


class PilotWorkflow1_CustomerDataLoad:
    """
    Pilot Workflow 1: Simple Customer Data Load
    Informatica Workflow: m_customer_load
    Source: Oracle customer table
    Target: Hive customer_dim table
    Transformations: Filter, Expression, Lookup, Aggregator
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.workflow_name = "customer_data_load"
        
    def extract_source_data(self, source_config: Dict[str, Any]) -> DataFrame:
        logger.info(f"Extracting source data for {self.workflow_name}")
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", source_config["jdbc_url"]) \
            .option("dbtable", source_config["table_name"]) \
            .option("user", source_config["username"]) \
            .option("password", source_config["password"]) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .option("fetchsize", "10000") \
            .option("numPartitions", "4") \
            .load()
        
        logger.info(f"Extracted {df.count()} records from source")
        return df
    
    def apply_transformations(self, source_df: DataFrame) -> DataFrame:
        logger.info("Applying transformations")
        
        # Filter Transformation: Filter active customers only
        filtered_df = source_df.filter(
            (F.col("status") == "ACTIVE") & 
            (F.col("customer_id").isNotNull())
        )
        
        # Expression Transformation: Calculate derived fields
        transformed_df = filtered_df.withColumn(
            "full_name",
            F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
        ).withColumn(
            "age",
            F.floor(F.months_between(F.current_date(), F.col("birth_date")) / 12)
        ).withColumn(
            "customer_key",
            F.sha2(F.concat_ws("|", F.col("customer_id"), F.col("source_system")), 256)
        ).withColumn(
            "load_date",
            F.current_timestamp()
        ).withColumn(
            "is_vip",
            F.when(F.col("total_purchases") > 10000, F.lit("Y")).otherwise(F.lit("N"))
        )
        
        # Lookup Transformation: Enrich with region data
        region_df = self.spark.table("reference.regions")
        enriched_df = transformed_df.alias("cust").join(
            region_df.alias("reg"),
            F.col("cust.region_id") == F.col("reg.region_id"),
            "left"
        ).select(
            "cust.*",
            F.col("reg.region_name"),
            F.col("reg.region_code")
        )
        
        # Aggregator Transformation: Calculate customer metrics
        window_spec = Window.partitionBy("customer_id").orderBy(F.col("transaction_date").desc())
        
        final_df = enriched_df.withColumn(
            "last_purchase_date",
            F.first("transaction_date").over(window_spec)
        ).withColumn(
            "purchase_count",
            F.count("transaction_id").over(Window.partitionBy("customer_id"))
        ).withColumn(
            "total_spend",
            F.sum("transaction_amount").over(Window.partitionBy("customer_id"))
        ).distinct()
        
        # Data Quality Checks
        final_df = final_df.withColumn(
            "data_quality_flag",
            F.when(
                (F.col("email").isNull()) | 
                (F.col("phone").isNull()), 
                F.lit("INCOMPLETE")
            ).otherwise(F.lit("COMPLETE"))
        )
        
        logger.info(f"Transformations applied, resulting in {final_df.count()} records")
        return final_df
    
    def load_target_data(self, transformed_df: DataFrame, target_config: Dict[str, Any]):
        logger.info("Loading data to target")
        
        target_table = target_config["target_table"]
        partition_columns = target_config.get("partition_columns", [])
        
        if partition_columns:
            transformed_df.write \
                .mode("overwrite") \
                .partitionBy(*partition_columns) \
                .format("parquet") \
                .option("compression", "snappy") \
                .saveAsTable(target_table)
        else:
            transformed_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("compression", "snappy") \
                .saveAsTable(target_table)
        
        logger.info(f"Data loaded to {target_table}")
    
    def execute(self, source_config: Dict[str, Any], target_config: Dict[str, Any]) -> Dict[str, Any]:
        start_time = time.time()
        
        try:
            source_df = self.extract_source_data(source_config)
            source_count = source_df.count()
            
            transformed_df = self.apply_transformations(source_df)
            target_count = transformed_df.count()
            
            self.load_target_data(transformed_df, target_config)
            
            execution_time = time.time() - start_time
            
            return {
                "status": "SUCCESS",
                "source_count": source_count,
                "target_count": target_count,
                "execution_time": execution_time,
                "records_per_second": target_count / execution_time if execution_time > 0 else 0
            }
        except Exception as e:
            logger.error(f"Workflow execution failed: {str(e)}")
            return {
                "status": "FAILED",
                "error": str(e),
                "execution_time": time.time() - start_time
            }


class PilotWorkflow2_SalesAggregation:
    """
    Pilot Workflow 2: Sales Data Aggregation
    Informatica Workflow: m_sales_aggregate
    Source: Hive sales_transactions table
    Target: Hive sales_summary table
    Transformations: Aggregator, Sorter, Expression, Router
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.workflow_name = "sales_aggregation"
        
    def extract_source_data(self, source_config: Dict[str, Any]) -> DataFrame:
        logger.info(f"Extracting source data for {self.workflow_name}")
        
        source_table = source_config["source_table"]
        partition_filter = source_config.get("partition_filter")
        
        df = self.spark.table(source_table)
        
        if partition_filter:
            df = df.filter(partition_filter)
        
        logger.info(f"Extracted {df.count()} records from source")
        return df
    
    def apply_transformations(self, source_df: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
        logger.info("Applying transformations")
        
        # Expression Transformation: Calculate derived metrics
        prepared_df = source_df.withColumn(
            "sale_amount",
            F.col("quantity") * F.col("unit_price")
        ).withColumn(
            "discount_amount",
            F.col("sale_amount") * F.col("discount_percent") / 100
        ).withColumn(
            "net_sale_amount",
            F.col("sale_amount") - F.col("discount_amount")
        ).withColumn(
            "sale_date",
            F.to_date(F.col("transaction_timestamp"))
        ).withColumn(
            "sale_year",
            F.year(F.col("sale_date"))
        ).withColumn(
            "sale_month",
            F.month(F.col("sale_date"))
        ).withColumn(
            "sale_quarter",
            F.quarter(F.col("sale_date"))
        )
        
        # Aggregator Transformation: Daily aggregation
        daily_agg = prepared_df.groupBy(
            "sale_date",
            "sale_year",
            "sale_month",
            "sale_quarter",
            "product_id",
            "store_id"
        ).agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("sale_amount").alias("total_sale_amount"),
            F.sum("discount_amount").alias("total_discount_amount"),
            F.sum("net_sale_amount").alias("total_net_sale_amount"),
            F.avg("unit_price").alias("avg_unit_price"),
            F.max("unit_price").alias("max_unit_price"),
            F.min("unit_price").alias("min_unit_price")
        )
        
        # Aggregator Transformation: Monthly aggregation
        monthly_agg = prepared_df.groupBy(
            "sale_year",
            "sale_month",
            "sale_quarter",
            "product_id",
            "store_id"
        ).agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("sale_amount").alias("total_sale_amount"),
            F.sum("discount_amount").alias("total_discount_amount"),
            F.sum("net_sale_amount").alias("total_net_sale_amount"),
            F.avg("unit_price").alias("avg_unit_price"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
        
        # Router Transformation: Split high value and regular sales
        high_value_threshold = 1000
        
        high_value_sales = daily_agg.filter(
            F.col("total_net_sale_amount") >= high_value_threshold
        ).withColumn("category", F.lit("HIGH_VALUE"))
        
        regular_sales = daily_agg.filter(
            F.col("total_net_sale_amount") < high_value_threshold
        ).withColumn("category", F.lit("REGULAR"))
        
        # Sorter Transformation: Sort by date and amount
        sorted_daily = daily_agg.orderBy(
            F.col("sale_date").desc(),
            F.col("total_net_sale_amount").desc()
        )
        
        sorted_monthly = monthly_agg.orderBy(
            F.col("sale_year").desc(),
            F.col("sale_month").desc(),
            F.col("total_net_sale_amount").desc()
        )
        
        # Add audit columns
        sorted_daily = sorted_daily.withColumn("load_timestamp", F.current_timestamp())
        sorted_monthly = sorted_monthly.withColumn("load_timestamp", F.current_timestamp())
        
        logger.info("Transformations applied successfully")
        return sorted_daily, sorted_monthly, high_value_sales.union(regular_sales)
    
    def load_target_data(self, daily_df: DataFrame, monthly_df: DataFrame, 
                        categorized_df: DataFrame, target_config: Dict[str, Any]):
        logger.info("Loading data to target tables")
        
        daily_df.write \
            .mode("overwrite") \
            .partitionBy("sale_year", "sale_month") \
            .format("parquet") \
            .saveAsTable(target_config["daily_table"])
        
        monthly_df.write \
            .mode("overwrite") \
            .partitionBy("sale_year", "sale_month") \
            .format("parquet") \
            .saveAsTable(target_config["monthly_table"])
        
        categorized_df.write \
            .mode("overwrite") \
            .partitionBy("category") \
            .format("parquet") \
            .saveAsTable(target_config["categorized_table"])
        
        logger.info("Data loaded to all target tables")
    
    def execute(self, source_config: Dict[str, Any], target_config: Dict[str, Any]) -> Dict[str, Any]:
        start_time = time.time()
        
        try:
            source_df = self.extract_source_data(source_config)
            source_count = source_df.count()
            
            daily_df, monthly_df, categorized_df = self.apply_transformations(source_df)
            
            self.load_target_data(daily_df, monthly_df, categorized_df, target_config)
            
            execution_time = time.time() - start_time
            
            return {
                "status": "SUCCESS",
                "source_count": source_count,
                "daily_count": daily_df.count(),
                "monthly_count": monthly_df.count(),
                "categorized_count": categorized_df.count(),
                "execution_time": execution_time
            }
        except Exception as e:
            logger.error(f"Workflow execution failed: {str(e)}")
            return {
                "status": "FAILED",
                "error": str(e),
                "execution_time": time.time() - start_time
            }


class DataValidator:
    """Comprehensive data validation framework"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def validate_record_count(self, source_df: DataFrame, target_df: DataFrame, 
                             tolerance: float = 0.0) -> ValidationResult:
        start_time = time.time()
        
        source_count = source_df.count()
        target_count = target_df.count()
        
        match_percentage = (min(source_count, target_count) / max(source_count, target_count) * 100) \
            if max(source_count, target_count) > 0 else 0
        
        status = ValidationStatus.PASSED if abs(source_count - target_count) <= tolerance \
            else ValidationStatus.FAILED
        
        discrepancies = []
        if status == ValidationStatus.FAILED:
            discrepancies.append({
                "type": "COUNT_MISMATCH",
                "source_count": source_count,
                "target_count": target_count,
                "difference": abs(source_count - target_count)
            })
        
        return ValidationResult(
            validation_type="RECORD_COUNT",
            status=status,
            source_count=source_count,
            target_count=target_count,
            match_percentage=match_percentage,
            discrepancies=discrepancies,
            execution_time_seconds=time.time() - start_time,
            timestamp=datetime.now().isoformat()
        )
    
    def validate_schema(self, source_df: DataFrame, target_df: DataFrame) -> ValidationResult:
        start_time = time.time()
        
        source_schema = {field.name: field.dataType.simpleString() for field in source_df.schema.fields}
        target_schema = {field.name: field.dataType.simpleString() for field in target_df.schema.fields}
        
        common_fields = set(source_schema.keys()) & set(target_schema.keys())
        missing_in_target = set(source_schema.keys()) - set(target_schema.keys())
        extra_in_target = set(target_schema.keys()) - set(source_schema.keys())
        
        type_mismatches = []
        for field in common_fields:
            if source_schema[field] != target_schema[field]:
                type_mismatches.append({
                    "field": field,
                    "source_type": source_schema[field],
                    "target_type": target_schema[field]
                })
        
        discrepancies = []
        if missing_in_target:
            discrepancies.append({"type": "MISSING_FIELDS", "fields": list(missing_in_target)})
        if extra_in_target:
            discrepancies.append({"type": "EXTRA_FIELDS", "fields": list(extra_in_target)})
        if type_mismatches:
            discrepancies.append({"type": "TYPE_MISMATCHES", "details": type_mismatches})
        
        status = ValidationStatus.PASSED if not discrepancies else ValidationStatus.WARNING
        
        match_percentage = (len(common_fields) / max(len(source_schema), len(target_schema)) * 100) \
            if max(len(source_schema), len(target_schema)) > 0 else 0
        
        return ValidationResult(
            validation_type="SCHEMA",
            status=status,
            source_count=len(source_schema),
            target_count=len(target_schema),
            match_percentage=match_percentage,
            discrepancies=discrepancies,
            execution_time_seconds=time.time() - start_time,
            timestamp=datetime.now().isoformat()
        )
    
    def validate_data_quality(self, source_df: DataFrame, target_df: DataFrame, 
                             key_columns: List[str]) -> ValidationResult:
        start_time = time.time()
        
        # Create composite key for comparison
        key_expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) 
                                        for c in key_columns])
        
        source_with_key = source_df.withColumn("composite_key", key_expr)
        target_with_key = target_df.withColumn("composite_key", key_expr)
        
        # Find records only in source
        only_in_source = source_with_key.join(
            target_with_key,
            "composite_key",
            "left_anti"
        ).count()
        
        # Find records only in target
        only_in_target = target_with_key.join(
            source_with_key,
            "composite_key",
            "left_anti"
        ).count()
        
        # Common records
        common_records = source_with_key.join(
            target_with_key,
            "composite_key",
            "inner"
        ).count()
        
        total_records = max(source_df.count(), target_df.count())
        match_percentage = (common_records / total_records * 100) if total_records > 0 else 0
        
        discrepancies = []
        if only_in_source > 0:
            discrepancies.append({
                "type": "MISSING_IN_TARGET",
                "count": only_in_source
            })
        if only_in_target > 0:
            discrepancies.append({
                "type": "EXTRA_IN_TARGET",
                "count": only_in_target
            })
        
        status = ValidationStatus.PASSED if match_percentage == 100.0 else ValidationStatus.FAILED
        
        return ValidationResult(
            validation_type="DATA_QUALITY",
            status=status,
            source_count=source_df.count(),
            target_count=target_df.count(),
            match_percentage=match_percentage,
            discrepancies=discrepancies,
            execution_time_seconds=time.time() - start_time,
            timestamp=datetime.now().isoformat()
        )
    
    def validate_aggregates(self, source_df: DataFrame, target_df: DataFrame, 
                           agg_columns: List[str], group_by_columns: List[str]) -> ValidationResult:
        start_time = time.time()
        
        # Aggregate source
        source_agg = source_df.groupBy(*group_by_columns).agg(
            *[F.sum(col).alias(f"source_{col}") for col in agg_columns]
        )
        
        # Aggregate target
        target_agg = target_df.groupBy(*group_by_columns).agg(
            *[F.sum(col).alias(f"target_{col}") for col in agg_columns]
        )
        
        # Join and compare
        comparison = source_agg.join(target_agg, group_by_columns, "outer")
        
        discrepancies = []
        for col in agg_columns:
            mismatches = comparison.filter(
                F.col(f"source_{col}") != F.col(f"target_{col}")
            ).count()
            
            if mismatches > 0:
                discrepancies.append({
                    "column": col,
                    "mismatches": mismatches
                })
        
        total_groups = comparison.count()
        matched_groups = comparison.filter(
            F.concat_ws("||", *[F.col(f"source_{col}") == F.col(f"target_{col}") 
                               for col in agg_columns])
        ).count()
        
        match_percentage = (matched_groups / total_groups * 100) if total_groups > 0 else 0
        
        status = ValidationStatus.PASSED if match_percentage == 100.0 else ValidationStatus.FAILED
        
        return ValidationResult(
            validation_type="AGGREGATES",
            status=status,
            source_count=total_groups,
            target_count=total_groups,
            match_percentage=match_percentage,
            discrepancies=discrepancies,
            execution_time_seconds=time.time() - start_time,
            timestamp=datetime.now().isoformat()
        )
    
    def validate_null_checks(self, df: DataFrame, non_null_columns: List[str]) -> ValidationResult:
        start_time = time.time()
        
        total_records = df.count()
        discrepancies = []
        
        for col in non_null_columns:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                discrepancies.append({
                    "column": col,
                    "null_count": null_count,
                    "null_percentage": (null_count / total_records * 100) if total_records > 0 else 0
                })
        
        status = ValidationStatus.PASSED if not discrepancies else ValidationStatus.FAILED
        match_percentage = 100.0 if not discrepancies else 0.0
        
        return ValidationResult(
            validation_type="NULL_CHECKS",
            status=status,
            source_count=total_records,
            target_count=total_records,
            match_percentage=match_percentage,
            discrepancies=discrepancies,
            execution_time_seconds=time.time() - start_time,
            timestamp=datetime.now().isoformat()
        )


class PerformanceTester:
    """Performance testing and benchmarking framework"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def measure_execution_time(self, workflow_func, *args, **kwargs) -> float:
        start_time = time.time()
        workflow_func(*args, **kwargs)
        return time.time() - start_time
    
    def get_memory_usage(self) -> float:
        # Get Spark executor memory usage
        sc = self.spark.sparkContext
        status = sc.statusTracker()
        executor_info = status.getExecutorInfos()
        
        total_memory = sum([exec_info.totalMemory() for exec_info in executor_info])
        return total_memory / (1024 * 1024)  # Convert to MB
    
    def compare_with_baseline(self, current_time: float, baseline_time: float,
                             records_processed: int) -> PerformanceMetrics:
        
        improvement = ((baseline_time - current_time) / baseline_time * 100) if baseline_time > 0 else 0
        records_per_second = records_processed / current_time if current_time > 0 else 0
        
        return PerformanceMetrics(
            workflow_name="pilot_workflow",
            execution_time_seconds=current_time,
            records_processed=records_processed,
            records_per_second=records_per_second,
            memory_usage_mb=self.get_memory_usage(),
            cpu_usage_percent=0.0,  # Would need OS-level metrics
            baseline_time_seconds=baseline_time,
            performance_improvement_percent=improvement
        )
    
    def run_performance_tests(self, workflow, source_config: Dict[str, Any],
                             target_config: Dict[str, Any], 
                             baseline_time: float) -> PerformanceMetrics:
        
        logger.info("Starting performance test")
        
        result = workflow.execute(source_config, target_config)
        
        metrics = self.compare_with_baseline(
            current_time=result["execution_time"],
            baseline_time=baseline_time,
            records_processed=result.get("source_count", 0)
        )
        
        logger.info(f"Performance test completed: {metrics.performance_improvement_percent:.2f}% improvement")
        return metrics


class OrchestrationManager:
    """Workflow orchestration and dependency management"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.workflows = {}
        self.execution_log = []
        
    def register_workflow(self, workflow_name: str, workflow_instance: Any):
        self.workflows[workflow_name] = workflow_instance
        logger.info(f"Registered workflow: {workflow_name}")
    
    def execute_workflow(self, workflow_name: str, source_config: Dict[str, Any],
                        target_config: Dict[str, Any]) -> Dict[str, Any]:
        
        if workflow_name not in self.workflows:
            raise ValueError(f"Workflow {workflow_name} not registered")
        
        workflow = self.workflows[workflow_name]
        
        execution_record = {
            "workflow_name": workflow_name,
            "start_time": datetime.now().isoformat(),
            "status": "RUNNING"
        }
        
        try:
            result = workflow.execute(source_config, target_config)
            execution_record["status"] = "SUCCESS"
            execution_record["result"] = result
            execution_record["end_time"] = datetime.now().isoformat()
            
        except Exception as e:
            execution_record["status"] = "FAILED"
            execution_record["error"] = str(e)
            execution_record["end_time"] = datetime.now().isoformat()
            logger.error(f"Workflow {workflow_name} failed: {str(e)}")
            raise
        
        finally:
            self.execution_log.append(execution_record)
        
        return execution_record
    
    def execute_workflow_dag(self, dag_config: List[Dict[str, Any]]):
        """Execute workflows in dependency order"""
        
        for workflow_config in dag_config:
            workflow_name = workflow_config["name"]
            dependencies = workflow_config.get("dependencies", [])
            
            # Check if dependencies completed successfully
            for dep in dependencies:
                dep_log = [log for log in self.execution_log if log["workflow_name"] == dep]
                if not dep_log or dep_log[-1]["status"] != "SUCCESS":
                    logger.error(f"Dependency {dep} not satisfied for {workflow_name}")
                    continue
            
            logger.info(f"Executing workflow: {workflow_name}")
            self.execute_workflow(
                workflow_name,
                workflow_config["source_config"],
                workflow_config["target_config"]
            )
    
    def get_execution_summary(self) -> Dict[str, Any]:
        total_runs = len(self.execution_log)
        successful_runs = len([log for log in self.execution_log if log["status"] == "SUCCESS"])
        failed_runs = len([log for log in self.execution_log if log["status"] == "FAILED"])
        
        return {
            "total_executions": total_runs,
            "successful": successful_runs,
            "failed": failed_runs,
            "success_rate": (successful_runs / total_runs * 100) if total_runs > 0 else 0,
            "execution_log": self.execution_log
        }


class LessonsLearnedDocumentation:
    """Document lessons learned and migration gaps"""
    
    def __init__(self):
        self.lessons = []
        
    def add_lesson(self, category: str, description: str, impact: str,
                   recommendation: str, priority: str):
        
        lesson = LessonLearned(
            category=category,
            description=description,
            impact=impact,
            recommendation=recommendation,
            priority=priority
        )
        self.lessons.append(lesson)
        logger.info(f"Documented lesson learned: {category} - {description}")
    
    def generate_report(self) -> Dict[str, Any]:
        return {
            "total_lessons": len(self.lessons),
            "by_category": self._group_by_category(),
            "by_priority": self._group_by_priority(),
            "lessons": [asdict(lesson) for lesson in self.lessons]
        }
    
    def _group_by_category(self) -> Dict[str, int]:
        categories = {}
        for lesson in self.lessons:
            categories[lesson.category] = categories.get(lesson.category, 0) + 1
        return categories
    
    def _group_by_priority(self) -> Dict[str, int]:
        priorities = {}
        for lesson in self.lessons:
            priorities[lesson.priority] = priorities.get(lesson.priority, 0) + 1
        return priorities