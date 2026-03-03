import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, trim, upper, lower, concat, when, coalesce,
    current_timestamp, sha2, concat_ws, count, sum as spark_sum,
    abs as spark_abs, max as spark_max
)
from pyspark.sql.types import StringType, IntegerType, DecimalType, DateType
from delta.tables import DeltaTable
import yaml
import json
from typing import Dict, List, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class WorkflowMetrics:
    """Metrics for workflow execution"""
    workflow_name: str
    start_time: datetime
    end_time: datetime
    records_processed: int
    records_inserted: int
    records_updated: int
    records_rejected: int
    status: str
    error_message: str = None


@dataclass
class ValidationResult:
    """Data validation result"""
    check_name: str
    source_count: int
    target_count: int
    match_percentage: float
    passed: bool
    details: Dict


class SparkSessionManager:
    """Manages Spark session with optimized configurations"""
    
    @staticmethod
    def create_session(app_name: str, configs: Dict = None) -> SparkSession:
        """Create optimized Spark session"""
        builder = SparkSession.builder.appName(app_name)
        
        default_configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.sql.shuffle.partitions": "200"
        }
        
        if configs:
            default_configs.update(configs)
        
        for key, value in default_configs.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        logger.info(f"Spark session created: {app_name}")
        return spark


class DataQualityValidator:
    """Validates data quality and reconciliation"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def validate_row_count(self, source_df: DataFrame, target_df: DataFrame, 
                          check_name: str) -> ValidationResult:
        """Validate row counts match"""
        source_count = source_df.count()
        target_count = target_df.count()
        match_pct = (min(source_count, target_count) / max(source_count, target_count) * 100) if max(source_count, target_count) > 0 else 0
        
        return ValidationResult(
            check_name=check_name,
            source_count=source_count,
            target_count=target_count,
            match_percentage=match_pct,
            passed=(source_count == target_count),
            details={"difference": abs(source_count - target_count)}
        )
    
    def validate_column_aggregates(self, source_df: DataFrame, target_df: DataFrame,
                                   columns: List[str], check_name: str) -> ValidationResult:
        """Validate aggregate values match for numeric columns"""
        results = []
        
        for column in columns:
            source_sum = source_df.agg(spark_sum(col(column))).collect()[0][0]
            target_sum = target_df.agg(spark_sum(col(column))).collect()[0][0]
            
            source_sum = source_sum if source_sum else 0
            target_sum = target_sum if target_sum else 0
            
            match = abs(float(source_sum) - float(target_sum)) < 0.01
            results.append({
                "column": column,
                "source_sum": float(source_sum),
                "target_sum": float(target_sum),
                "match": match
            })
        
        all_match = all(r["match"] for r in results)
        
        return ValidationResult(
            check_name=check_name,
            source_count=len(columns),
            target_count=len([r for r in results if r["match"]]),
            match_percentage=(len([r for r in results if r["match"]]) / len(columns) * 100),
            passed=all_match,
            details={"column_results": results}
        )
    
    def validate_data_hash(self, source_df: DataFrame, target_df: DataFrame,
                          key_columns: List[str], check_name: str) -> ValidationResult:
        """Validate data using hash comparison"""
        source_hash = source_df.withColumn(
            "row_hash",
            sha2(concat_ws("|", *[coalesce(col(c).cast("string"), lit("NULL")) for c in source_df.columns]), 256)
        ).select(key_columns + ["row_hash"])
        
        target_hash = target_df.withColumn(
            "row_hash",
            sha2(concat_ws("|", *[coalesce(col(c).cast("string"), lit("NULL")) for c in target_df.columns]), 256)
        ).select(key_columns + ["row_hash"])
        
        joined = source_hash.alias("src").join(
            target_hash.alias("tgt"),
            key_columns,
            "full_outer"
        )
        
        mismatches = joined.filter(
            (col("src.row_hash") != col("tgt.row_hash")) |
            col("src.row_hash").isNull() |
            col("tgt.row_hash").isNull()
        ).count()
        
        total_count = source_hash.count()
        match_pct = ((total_count - mismatches) / total_count * 100) if total_count > 0 else 0
        
        return ValidationResult(
            check_name=check_name,
            source_count=total_count,
            target_count=total_count - mismatches,
            match_percentage=match_pct,
            passed=(mismatches == 0),
            details={"mismatched_records": mismatches}
        )


class PilotWorkflow1_CustomerLoad:
    """
    Pilot Workflow 1: Simple Customer Data Load
    Migrated from Informatica PowerCenter
    
    Source: Customer flat files
    Target: Delta Lake customer dimension table
    
    Transformations:
    - Expression: Data cleansing and standardization
    - Filter: Remove invalid records
    - Lookup: Enrich with reference data
    - Update Strategy: Insert/Update logic
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        self.spark = spark
        self.config = config
        self.workflow_name = "Customer_Load_Workflow"
        self.metrics = None
    
    def read_source_data(self, source_path: str) -> DataFrame:
        """Source Qualifier: Read customer source data"""
        logger.info(f"Reading source data from: {source_path}")
        
        df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", ",") \
            .option("quote", '"') \
            .option("escape", '"') \
            .load(source_path)
        
        logger.info(f"Source records read: {df.count()}")
        return df
    
    def expression_transformation(self, df: DataFrame) -> DataFrame:
        """Expression Transformation: Data cleansing and standardization"""
        logger.info("Applying expression transformations")
        
        transformed_df = df.select(
            trim(col("CUSTOMER_ID")).alias("customer_id"),
            trim(upper(col("FIRST_NAME"))).alias("first_name"),
            trim(upper(col("LAST_NAME"))).alias("last_name"),
            trim(lower(col("EMAIL"))).alias("email"),
            trim(col("PHONE")).alias("phone"),
            trim(upper(col("ADDRESS"))).alias("address"),
            trim(upper(col("CITY"))).alias("city"),
            trim(upper(col("STATE"))).alias("state"),
            trim(col("ZIP_CODE")).alias("zip_code"),
            trim(upper(col("COUNTRY"))).alias("country"),
            col("REGISTRATION_DATE").cast(DateType()).alias("registration_date"),
            concat(
                trim(upper(col("FIRST_NAME"))),
                lit(" "),
                trim(upper(col("LAST_NAME")))
            ).alias("full_name"),
            current_timestamp().alias("load_timestamp"),
            lit("PILOT_LOAD").alias("source_system")
        )
        
        return transformed_df
    
    def filter_transformation(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Filter Transformation: Separate valid and invalid records"""
        logger.info("Applying filter transformations")
        
        valid_df = df.filter(
            col("customer_id").isNotNull() &
            (col("customer_id") != "") &
            col("first_name").isNotNull() &
            col("last_name").isNotNull() &
            col("email").isNotNull() &
            col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
        )
        
        rejected_df = df.join(valid_df, on=df.columns, how="left_anti")
        
        logger.info(f"Valid records: {valid_df.count()}, Rejected records: {rejected_df.count()}")
        
        return valid_df, rejected_df
    
    def lookup_transformation(self, df: DataFrame, lookup_path: str) -> DataFrame:
        """Lookup Transformation: Enrich with reference data"""
        logger.info("Applying lookup transformations")
        
        lookup_df = self.spark.read.format("delta").load(lookup_path) \
            .select(
                col("state").alias("lookup_state"),
                col("state_code"),
                col("region")
            )
        
        enriched_df = df.join(
            lookup_df,
            df.state == lookup_df.lookup_state,
            "left"
        ).drop("lookup_state")
        
        enriched_df = enriched_df.withColumn(
            "state_code",
            coalesce(col("state_code"), lit("UNKNOWN"))
        ).withColumn(
            "region",
            coalesce(col("region"), lit("UNKNOWN"))
        )
        
        return enriched_df
    
    def update_strategy_transformation(self, df: DataFrame, target_path: str) -> WorkflowMetrics:
        """Update Strategy: Implement SCD Type 1 logic"""
        logger.info("Applying update strategy")
        
        start_time = datetime.now()
        records_processed = df.count()
        records_inserted = 0
        records_updated = 0
        
        try:
            if DeltaTable.isDeltaTable(self.spark, target_path):
                delta_table = DeltaTable.forPath(self.spark, target_path)
                
                # Merge logic: Update existing, Insert new
                delta_table.alias("target").merge(
                    df.alias("source"),
                    "target.customer_id = source.customer_id"
                ).whenMatchedUpdate(set={
                    "first_name": "source.first_name",
                    "last_name": "source.last_name",
                    "email": "source.email",
                    "phone": "source.phone",
                    "address": "source.address",
                    "city": "source.city",
                    "state": "source.state",
                    "zip_code": "source.zip_code",
                    "country": "source.country",
                    "registration_date": "source.registration_date",
                    "full_name": "source.full_name",
                    "load_timestamp": "source.load_timestamp",
                    "state_code": "source.state_code",
                    "region": "source.region"
                }).whenNotMatchedInsertAll().execute()
                
                # Get metrics from Delta log
                history = delta_table.history(1).collect()[0]
                records_updated = history["operationMetrics"].get("numTargetRowsUpdated", 0)
                records_inserted = history["operationMetrics"].get("numTargetRowsInserted", 0)
                
            else:
                # Initial load
                df.write.format("delta").mode("overwrite").save(target_path)
                records_inserted = records_processed
            
            end_time = datetime.now()
            
            self.metrics = WorkflowMetrics(
                workflow_name=self.workflow_name,
                start_time=start_time,
                end_time=end_time,
                records_processed=records_processed,
                records_inserted=records_inserted,
                records_updated=records_updated,
                records_rejected=0,
                status="SUCCESS"
            )
            
            logger.info(f"Target load complete. Inserted: {records_inserted}, Updated: {records_updated}")
            
            return self.metrics
            
        except Exception as e:
            logger.error(f"Error in update strategy: {str(e)}")
            end_time = datetime.now()
            
            self.metrics = WorkflowMetrics(
                workflow_name=self.workflow_name,
                start_time=start_time,
                end_time=end_time,
                records_processed=records_processed,
                records_inserted=records_inserted,
                records_updated=records_updated,
                records_rejected=0,
                status="FAILED",
                error_message=str(e)
            )
            
            raise
    
    def execute(self, source_path: str, target_path: str, lookup_path: str, 
                rejected_path: str) -> WorkflowMetrics:
        """Execute complete workflow"""
        logger.info(f"Starting workflow: {self.workflow_name}")
        
        # Step 1: Read source data
        source_df = self.read_source_data(source_path)
        
        # Step 2: Apply expression transformations
        transformed_df = self.expression_transformation(source_df)
        
        # Step 3: Filter valid and invalid records
        valid_df, rejected_df = self.filter_transformation(transformed_df)
        
        # Step 4: Write rejected records
        if rejected_df.count() > 0:
            rejected_df.write.format("delta").mode("append").save(rejected_path)
            logger.info(f"Rejected records written to: {rejected_path}")
        
        # Step 5: Lookup enrichment
        enriched_df = self.lookup_transformation(valid_df, lookup_path)
        
        # Step 6: Load target with update strategy
        metrics = self.update_strategy_transformation(enriched_df, target_path)
        
        logger.info(f"Workflow completed: {self.workflow_name}")
        return metrics


class PilotWorkflow2_SalesAggregation:
    """
    Pilot Workflow 2: Sales Data Aggregation
    Migrated from Informatica PowerCenter
    
    Source: Delta Lake sales transactions
    Target: Delta Lake sales summary table
    
    Transformations:
    - Aggregator: Group by customer and calculate metrics
    - Expression: Derive calculated fields
    - Joiner: Join with customer dimension
    - Sorter: Sort results
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        self.spark = spark
        self.config = config
        self.workflow_name = "Sales_Aggregation_Workflow"
        self.metrics = None
    
    def read_source_transactions(self, source_path: str) -> DataFrame:
        """Source Qualifier: Read sales transactions"""
        logger.info(f"Reading sales transactions from: {source_path}")
        
        df = self.spark.read.format("delta").load(source_path)
        
        logger.info(f"Transaction records read: {df.count()}")
        return df
    
    def aggregator_transformation(self, df: DataFrame) -> DataFrame:
        """Aggregator Transformation: Calculate sales metrics by customer"""
        logger.info("Applying aggregator transformations")
        
        aggregated_df = df.groupBy(
            "customer_id",
            "product_category"
        ).agg(
            count("*").alias("transaction_count"),
            spark_sum("sale_amount").alias("total_sales"),
            spark_sum("quantity").alias("total_quantity"),
            spark_max("transaction_date").alias("last_transaction_date")
        )
        
        return aggregated_df
    
    def expression_transformation(self, df: DataFrame) -> DataFrame:
        """Expression Transformation: Derive calculated fields"""
        logger.info("Applying expression transformations for calculations")
        
        calculated_df = df.select(
            col("customer_id"),
            col("product_category"),
            col("transaction_count"),
            col("total_sales"),
            col("total_quantity"),
            col("last_transaction_date"),
            (col("total_sales") / col("transaction_count")).alias("avg_transaction_value"),
            (col("total_quantity") / col("transaction_count")).alias("avg_items_per_transaction"),
            when(col("total_sales") >= 10000, "PLATINUM")
            .when(col("total_sales") >= 5000, "GOLD")
            .when(col("total_sales") >= 1000, "SILVER")
            .otherwise("BRONZE").alias("customer_tier"),
            current_timestamp().alias("aggregation_timestamp")
        )
        
        return calculated_df
    
    def joiner_transformation(self, df: DataFrame, customer_path: str) -> DataFrame:
        """Joiner Transformation: Join with customer dimension"""
        logger.info("Applying joiner transformation with customer dimension")
        
        customer_df = self.spark.read.format("delta").load(customer_path) \
            .select(
                col("customer_id").alias("cust_id"),
                col("full_name"),
                col("email"),
                col("state"),
                col("region")
            )
        
        joined_df = df.join(
            customer_df,
            df.customer_id == customer_df.cust_id,
            "inner"
        ).drop("cust_id")
        
        return joined_df
    
    def sorter_transformation(self, df: DataFrame) -> DataFrame:
        """Sorter Transformation: Sort by sales descending"""
        logger.info("Applying sorter transformation")
        
        sorted_df = df.orderBy(col("total_sales").desc())
        
        return sorted_df
    
    def write_target(self, df: DataFrame, target_path: str) -> WorkflowMetrics:
        """Write aggregated results to target"""
        logger.info("Writing aggregated results to target")
        
        start_time = datetime.now()
        records_processed = df.count()
        
        try:
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(target_path)
            
            end_time = datetime.now()
            
            self.metrics = WorkflowMetrics(
                workflow_name=self.workflow_name,
                start_time=start_time,
                end_time=end_time,
                records_processed=records_processed,
                records_inserted=records_processed,
                records_updated=0,
                records_rejected=0,
                status="SUCCESS"
            )
            
            logger.info(f"Target write complete. Records written: {records_processed}")
            
            return self.metrics
            
        except Exception as e:
            logger.error(f"Error writing to target: {str(e)}")
            end_time = datetime.now()
            
            self.metrics = WorkflowMetrics(
                workflow_name=self.workflow_name,
                start_time=start_time,
                end_time=end_time,
                records_processed=records_processed,
                records_inserted=0,
                records_updated=0,
                records_rejected=0,
                status="FAILED",
                error_message=str(e)
            )
            
            raise
    
    def execute(self, source_path: str, customer_path: str, target_path: str) -> WorkflowMetrics:
        """Execute complete workflow"""
        logger.info(f"Starting workflow: {self.workflow_name}")
        
        # Step 1: Read source transactions
        transactions_df = self.read_source_transactions(source_path)
        
        # Step 2: Aggregate by customer and category
        aggregated_df = self.aggregator_transformation(transactions_df)
        
        # Step 3: Calculate derived metrics
        calculated_df = self.expression_transformation(aggregated_df)
        
        # Step 4: Join with customer dimension
        enriched_df = self.joiner_transformation(calculated_df, customer_path)
        
        # Step 5: Sort results
        sorted_df = self.sorter_transformation(enriched_df)
        
        # Step 6: Write to target
        metrics = self.write_target(sorted_df, target_path)
        
        logger.info(f"Workflow completed: {self.workflow_name}")
        return metrics


class PerformanceMonitor:
    """Monitor and compare workflow performance"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def capture_performance_metrics(self, metrics: WorkflowMetrics) -> Dict:
        """Capture detailed performance metrics"""
        duration_seconds = (metrics.end_time - metrics.start_time).total_seconds()
        
        performance = {
            "workflow_name": metrics.workflow_name,
            "execution_date": metrics.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": duration_seconds,
            "duration_minutes": duration_seconds / 60,
            "records_processed": metrics.records_processed,
            "records_inserted": metrics.records_inserted,
            "records_updated": metrics.records_updated,
            "records_rejected": metrics.records_rejected,
            "throughput_records_per_second": metrics.records_processed / duration_seconds if duration_seconds > 0 else 0,
            "status": metrics.status,
            "error_message": metrics.error_message
        }
        
        return performance
    
    def compare_with_baseline(self, current_metrics: Dict, baseline_metrics: Dict) -> Dict:
        """Compare current performance with PowerCenter baseline"""
        comparison = {
            "workflow_name": current_metrics["workflow_name"],
            "pyspark_duration_seconds": current_metrics["duration_seconds"],
            "powercenter_duration_seconds": baseline_metrics.get("duration_seconds", 0),
            "performance_improvement_pct": 0,
            "pyspark_throughput": current_metrics["throughput_records_per_second"],
            "powercenter_throughput": baseline_metrics.get("throughput_records_per_second", 0),
            "throughput_improvement_pct": 0,
            "meets_baseline": False
        }
        
        if baseline_metrics.get("duration_seconds", 0) > 0:
            improvement = ((baseline_metrics["duration_seconds"] - current_metrics["duration_seconds"]) 
                          / baseline_metrics["duration_seconds"] * 100)
            comparison["performance_improvement_pct"] = improvement
            comparison["meets_baseline"] = current_metrics["duration_seconds"] <= baseline_metrics["duration_seconds"]
        
        if baseline_metrics.get("throughput_records_per_second", 0) > 0:
            throughput_improvement = ((current_metrics["throughput_records_per_second"] - 
                                      baseline_metrics["throughput_records_per_second"]) 
                                     / baseline_metrics["throughput_records_per_second"] * 100)
            comparison["throughput_improvement_pct"] = throughput_improvement
        
        return comparison


class PilotOrchestrator:
    """Orchestrate pilot workflow execution with monitoring and validation"""
    
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.spark = SparkSessionManager.create_session(
            "Pilot_Migration_POC",
            self.config.get("spark_configs", {})
        )
        
        self.validator = DataQualityValidator(self.spark)
        self.monitor = PerformanceMonitor(self.spark)
        self.results = []
    
    def execute_workflow_1(self) -> Dict:
        """Execute Customer Load workflow with validation"""
        logger.info("=" * 80)
        logger.info("Executing Pilot Workflow 1: Customer Load")
        logger.info("=" * 80)
        
        workflow_config = self.config["workflows"]["customer_load"]
        
        workflow = PilotWorkflow1_CustomerLoad(self.spark, workflow_config)
        
        try:
            # Execute workflow
            metrics = workflow.execute(
                source_path=workflow_config["source_path"],
                target_path=workflow_config["target_path"],
                lookup_path=workflow_config["lookup_path"],
                rejected_path=workflow_config["rejected_path"]
            )
            
            # Capture performance metrics
            performance = self.monitor.capture_performance_metrics(metrics)
            
            # Data validation
            source_df = self.spark.read.format("csv") \
                .option("header", "true") \
                .load(workflow_config["source_path"])
            
            target_df = self.spark.read.format("delta").load(workflow_config["target_path"])
            
            validation_results = []
            
            # Row count validation
            row_count_result = self.validator.validate_row_count(
                source_df,
                target_df,
                "Customer Load - Row Count"
            )
            validation_results.append(row_count_result)
            
            # Performance comparison with baseline
            baseline = workflow_config.get("baseline_metrics", {})
            performance_comparison = self.monitor.compare_with_baseline(performance, baseline)
            
            result = {
                "workflow_name": "Customer_Load_Workflow",
                "status": metrics.status,
                "performance": performance,
                "performance_comparison": performance_comparison,
                "validation_results": [vars(vr) for vr in validation_results],
                "all_validations_passed": all(vr.passed for vr in validation_results)
            }
            
            self.results.append(result)
            
            logger.info(f"Workflow 1 Status: {metrics.status}")
            logger.info(f"Performance: {performance['duration_seconds']:.2f}s, "
                       f"{performance['throughput_records_per_second']:.2f} records/sec")
            logger.info(f"Validation Passed: {result['all_validations_passed']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Workflow 1 failed: {str(e)}")
            result = {
                "workflow_name": "Customer_Load_Workflow",
                "status": "FAILED",
                "error": str(e)
            }
            self.results.append(result)
            return result
    
    def execute_workflow_2(self) -> Dict:
        """Execute Sales Aggregation workflow with validation"""
        logger.info("=" * 80)
        logger.info("Executing Pilot Workflow 2: Sales Aggregation")
        logger.info("=" * 80)
        
        workflow_config = self.config["workflows"]["sales_aggregation"]
        
        workflow = PilotWorkflow2_SalesAggregation(self.spark, workflow_config)
        
        try:
            # Execute workflow
            metrics = workflow.execute(
                source_path=workflow_config["source_path"],
                customer_path=workflow_config["customer_path"],
                target_path=workflow_config["target_path"]
            )
            
            # Capture performance metrics
            performance = self.monitor.capture_performance_metrics(metrics)
            
            # Data validation
            target_df = self.spark.read.format("delta").load(workflow_config["target_path"])
            
            validation_results = []
            
            # Validate aggregation logic
            source_df = self.spark.read.format("delta").load(workflow_config["source_path"])
            
            expected_count = source_df.select("customer_id", "product_category").distinct().count()
            actual_count = target_df.count()
            
            agg_validation = ValidationResult(
                check_name="Sales Aggregation - Group By Count",
                source_count=expected_count,
                target_count=actual_count,
                match_percentage=(min(expected_count, actual_count) / max(expected_count, actual_count) * 100),
                passed=(expected_count == actual_count),
                details={"expected": expected_count, "actual": actual_count}
            )
            validation_results.append(agg_validation)
            
            # Performance comparison with baseline
            baseline = workflow_config.get("baseline_metrics", {})
            performance_comparison = self.monitor.compare_with_baseline(performance, baseline)
            
            result = {
                "workflow_name": "Sales_Aggregation_Workflow",
                "status": metrics.status,
                "performance": performance,
                "performance_comparison": performance_comparison,
                "validation_results": [vars(vr) for vr in validation_results],
                "all_validations_passed": all(vr.passed for vr in validation_results)
            }
            
            self.results.append(result)
            
            logger.info(f"Workflow 2 Status: {metrics.status}")
            logger.info(f"Performance: {performance['duration_seconds']:.2f}s, "
                       f"{performance['throughput_records_per_second']:.2f} records/sec")
            logger.info(f"Validation Passed: {result['all_validations_passed']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Workflow 2 failed: {str(e)}")
            result = {
                "workflow_name": "Sales_Aggregation_Workflow",
                "status": "FAILED",
                "error": str(e)
            }
            self.results.append(result)
            return result
    
    def generate_pilot_report(self, output_path: str):
        """Generate comprehensive pilot migration report"""
        logger.info("Generating pilot migration report")
        
        report = {
            "pilot_execution_summary": {
                "execution_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_workflows": len(self.results),
                "successful_workflows": len([r for r in self.results if r["status"] == "SUCCESS"]),
                "failed_workflows": len([r for r in self.results if r["status"] == "FAILED"]),
                "validation_pass_rate": len([r for r in self.results if r.get("all_validations_passed", False)]) / len(self.results) * 100 if self.results else 0
            },
            "workflow_results": self.results,
            "lessons_learned": self.config.get("lessons_learned", []),
            "identified_gaps": self.