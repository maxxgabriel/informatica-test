import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import hashlib
import json
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ValidationStatus(Enum):
    """Enumeration for validation status"""
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"


@dataclass
class ValidationResult:
    """Data class to store validation results"""
    workflow_name: str
    validation_type: str
    status: ValidationStatus
    source_count: int
    target_count: int
    match_percentage: float
    discrepancies: List[Dict]
    execution_time: float
    timestamp: str
    
    def to_dict(self):
        return asdict(self)


@dataclass
class PerformanceMetrics:
    """Data class to store performance metrics"""
    workflow_name: str
    source_system: str
    target_system: str
    source_execution_time: float
    target_execution_time: float
    improvement_percentage: float
    records_processed: int
    throughput_source: float
    throughput_target: float
    timestamp: str
    
    def to_dict(self):
        return asdict(self)


class PilotMigrationFramework:
    """
    Framework for conducting pilot migrations from Informatica PowerCenter to PySpark
    Handles workflow migration, validation, and performance testing
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize pilot migration framework
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary containing connection details and paths
        """
        self.spark = spark
        self.config = config
        self.validation_results = []
        self.performance_metrics = []
        self.lessons_learned = []
        
    def read_source_data(self, table_name: str, source_query: str = None) -> Any:
        """
        Read data from source system (simulating Informatica PowerCenter output)
        
        Args:
            table_name: Name of the source table
            source_query: Optional SQL query for source data
            
        Returns:
            DataFrame containing source data
        """
        try:
            logger.info(f"Reading source data from table: {table_name}")
            
            if source_query:
                df = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.config['source_jdbc_url']) \
                    .option("query", source_query) \
                    .option("user", self.config['source_user']) \
                    .option("password", self.config['source_password']) \
                    .option("driver", self.config['source_driver']) \
                    .load()
            else:
                df = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.config['source_jdbc_url']) \
                    .option("dbtable", table_name) \
                    .option("user", self.config['source_user']) \
                    .option("password", self.config['source_password']) \
                    .option("driver", self.config['source_driver']) \
                    .load()
            
            logger.info(f"Successfully read {df.count()} records from source")
            return df
            
        except Exception as e:
            logger.error(f"Error reading source data: {str(e)}")
            raise
    
    def write_target_data(self, df: Any, table_name: str, mode: str = "overwrite"):
        """
        Write data to target system
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            mode: Write mode (overwrite, append, etc.)
        """
        try:
            logger.info(f"Writing data to target table: {table_name}")
            
            df.write \
                .format("jdbc") \
                .option("url", self.config['target_jdbc_url']) \
                .option("dbtable", table_name) \
                .option("user", self.config['target_user']) \
                .option("password", self.config['target_password']) \
                .option("driver", self.config['target_driver']) \
                .mode(mode) \
                .save()
            
            logger.info(f"Successfully wrote {df.count()} records to target")
            
        except Exception as e:
            logger.error(f"Error writing target data: {str(e)}")
            raise


class PilotWorkflow1_CustomerDimensionLoad(PilotMigrationFramework):
    """
    Pilot Workflow 1: Simple Customer Dimension Load
    
    Source: Informatica PowerCenter Workflow
    Description: Loads customer dimension with basic transformations
    Transformations:
        - Source Qualifier
        - Expression (data cleansing and standardization)
        - Lookup (reference data)
        - Filter (active customers only)
        - Router (customer segmentation)
        - Target Load
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.workflow_name = "WF_CUSTOMER_DIM_LOAD"
        
    def source_qualifier_transformation(self) -> Any:
        """
        Replicate Informatica Source Qualifier transformation
        Reads from source customer table with filtering
        """
        logger.info(f"Executing Source Qualifier for {self.workflow_name}")
        
        source_query = """
            SELECT 
                CUSTOMER_ID,
                FIRST_NAME,
                LAST_NAME,
                EMAIL,
                PHONE,
                ADDRESS,
                CITY,
                STATE,
                ZIP_CODE,
                CUSTOMER_STATUS,
                CUSTOMER_TYPE,
                REGISTRATION_DATE,
                LAST_UPDATE_DATE
            FROM CUSTOMERS
            WHERE LAST_UPDATE_DATE >= CURRENT_DATE - 1
        """
        
        df = self.read_source_data("CUSTOMERS", source_query)
        return df
    
    def expression_transformation(self, df: Any) -> Any:
        """
        Replicate Informatica Expression transformation
        Data cleansing, standardization, and derived columns
        """
        logger.info("Executing Expression transformation")
        
        df_transformed = df.select(
            col("CUSTOMER_ID").cast(IntegerType()).alias("CUSTOMER_ID"),
            
            # Name standardization - trim and proper case
            initcap(trim(col("FIRST_NAME"))).alias("FIRST_NAME"),
            initcap(trim(col("LAST_NAME"))).alias("LAST_NAME"),
            
            # Full name concatenation
            concat_ws(" ", 
                     initcap(trim(col("FIRST_NAME"))), 
                     initcap(trim(col("LAST_NAME")))
            ).alias("FULL_NAME"),
            
            # Email standardization - lowercase and trim
            lower(trim(col("EMAIL"))).alias("EMAIL"),
            
            # Phone standardization - remove special characters
            regexp_replace(col("PHONE"), "[^0-9]", "").alias("PHONE_CLEAN"),
            
            # Address standardization
            upper(trim(col("ADDRESS"))).alias("ADDRESS"),
            upper(trim(col("CITY"))).alias("CITY"),
            upper(trim(col("STATE"))).alias("STATE"),
            trim(col("ZIP_CODE")).alias("ZIP_CODE"),
            
            # Status and type
            col("CUSTOMER_STATUS"),
            col("CUSTOMER_TYPE"),
            
            # Derived columns
            when(datediff(current_date(), col("REGISTRATION_DATE")) <= 90, "NEW")
            .when(datediff(current_date(), col("REGISTRATION_DATE")) <= 365, "REGULAR")
            .otherwise("VETERAN").alias("CUSTOMER_TENURE"),
            
            # Date fields
            col("REGISTRATION_DATE"),
            col("LAST_UPDATE_DATE"),
            
            # Audit columns
            current_timestamp().alias("ETL_INSERT_DATE"),
            lit(self.workflow_name).alias("ETL_PROCESS_NAME")
        )
        
        return df_transformed
    
    def lookup_transformation(self, df: Any) -> Any:
        """
        Replicate Informatica Lookup transformation
        Lookup reference data for state codes and customer segments
        """
        logger.info("Executing Lookup transformation")
        
        # Create lookup table for state codes
        state_lookup_df = self.spark.createDataFrame([
            ("AL", "Alabama", "South"),
            ("AK", "Alaska", "West"),
            ("CA", "California", "West"),
            ("NY", "New York", "Northeast"),
            ("TX", "Texas", "South"),
            ("FL", "Florida", "South")
        ], ["STATE_CODE", "STATE_NAME", "REGION"])
        
        # Perform lookup join
        df_with_lookup = df.join(
            state_lookup_df,
            df.STATE == state_lookup_df.STATE_CODE,
            "left"
        ).select(
            df["*"],
            state_lookup_df["STATE_NAME"],
            state_lookup_df["REGION"]
        )
        
        return df_with_lookup
    
    def filter_transformation(self, df: Any) -> Any:
        """
        Replicate Informatica Filter transformation
        Filter for active customers only
        """
        logger.info("Executing Filter transformation")
        
        df_filtered = df.filter(
            (col("CUSTOMER_STATUS") == "ACTIVE") &
            (col("EMAIL").isNotNull()) &
            (col("PHONE_CLEAN").isNotNull())
        )
        
        logger.info(f"Filtered records: {df_filtered.count()}")
        return df_filtered
    
    def router_transformation(self, df: Any) -> Tuple[Any, Any, Any]:
        """
        Replicate Informatica Router transformation
        Route customers to different segments based on criteria
        """
        logger.info("Executing Router transformation")
        
        # Premium customers
        premium_df = df.filter(col("CUSTOMER_TYPE") == "PREMIUM")
        
        # Standard customers
        standard_df = df.filter(col("CUSTOMER_TYPE") == "STANDARD")
        
        # Basic customers (default group)
        basic_df = df.filter(
            (col("CUSTOMER_TYPE") != "PREMIUM") & 
            (col("CUSTOMER_TYPE") != "STANDARD")
        )
        
        logger.info(f"Premium customers: {premium_df.count()}")
        logger.info(f"Standard customers: {standard_df.count()}")
        logger.info(f"Basic customers: {basic_df.count()}")
        
        return premium_df, standard_df, basic_df
    
    def execute_workflow(self) -> Dict[str, Any]:
        """
        Execute complete workflow with all transformations
        """
        logger.info(f"Starting execution of {self.workflow_name}")
        start_time = datetime.now()
        
        try:
            # Execute transformation pipeline
            df_source = self.source_qualifier_transformation()
            df_expression = self.expression_transformation(df_source)
            df_lookup = self.lookup_transformation(df_expression)
            df_filtered = self.filter_transformation(df_lookup)
            premium_df, standard_df, basic_df = self.router_transformation(df_filtered)
            
            # Add segment identifier and union all segments
            premium_df = premium_df.withColumn("CUSTOMER_SEGMENT", lit("PREMIUM"))
            standard_df = standard_df.withColumn("CUSTOMER_SEGMENT", lit("STANDARD"))
            basic_df = basic_df.withColumn("CUSTOMER_SEGMENT", lit("BASIC"))
            
            final_df = premium_df.union(standard_df).union(basic_df)
            
            # Write to target
            self.write_target_data(final_df, "DIM_CUSTOMER")
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            result = {
                "workflow_name": self.workflow_name,
                "status": "SUCCESS",
                "records_processed": final_df.count(),
                "execution_time": execution_time,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Workflow completed successfully in {execution_time} seconds")
            return result
            
        except Exception as e:
            logger.error(f"Workflow execution failed: {str(e)}")
            raise


class PilotWorkflow2_SalesFactLoad(PilotMigrationFramework):
    """
    Pilot Workflow 2: Sales Fact Load with Aggregation
    
    Source: Informatica PowerCenter Workflow
    Description: Loads sales fact table with aggregations
    Transformations:
        - Source Qualifier (multiple sources)
        - Joiner (sales and product data)
        - Aggregator (daily sales summary)
        - Expression (calculations)
        - Update Strategy (SCD Type 1)
        - Target Load
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.workflow_name = "WF_SALES_FACT_LOAD"
    
    def source_qualifier_sales(self) -> Any:
        """Source Qualifier for sales transactions"""
        logger.info("Reading sales transaction data")
        
        query = """
            SELECT 
                TRANSACTION_ID,
                CUSTOMER_ID,
                PRODUCT_ID,
                TRANSACTION_DATE,
                QUANTITY,
                UNIT_PRICE,
                DISCOUNT_PERCENT,
                TAX_AMOUNT,
                TRANSACTION_STATUS
            FROM SALES_TRANSACTIONS
            WHERE TRANSACTION_DATE >= CURRENT_DATE - 1
        """
        
        return self.read_source_data("SALES_TRANSACTIONS", query)
    
    def source_qualifier_product(self) -> Any:
        """Source Qualifier for product dimension"""
        logger.info("Reading product dimension data")
        
        return self.read_source_data("PRODUCTS")
    
    def joiner_transformation(self, sales_df: Any, product_df: Any) -> Any:
        """
        Replicate Informatica Joiner transformation
        Join sales with product data
        """
        logger.info("Executing Joiner transformation")
        
        joined_df = sales_df.join(
            product_df,
            sales_df.PRODUCT_ID == product_df.PRODUCT_ID,
            "inner"
        ).select(
            sales_df["*"],
            product_df["PRODUCT_NAME"],
            product_df["CATEGORY"],
            product_df["SUBCATEGORY"],
            product_df["COST_PRICE"]
        )
        
        return joined_df
    
    def expression_transformation(self, df: Any) -> Any:
        """
        Replicate Informatica Expression transformation
        Calculate derived metrics
        """
        logger.info("Executing Expression transformation")
        
        df_calculated = df.select(
            col("TRANSACTION_ID"),
            col("CUSTOMER_ID"),
            col("PRODUCT_ID"),
            col("TRANSACTION_DATE"),
            col("QUANTITY"),
            col("UNIT_PRICE"),
            col("DISCOUNT_PERCENT"),
            col("TAX_AMOUNT"),
            col("PRODUCT_NAME"),
            col("CATEGORY"),
            col("SUBCATEGORY"),
            col("COST_PRICE"),
            
            # Calculated fields
            (col("QUANTITY") * col("UNIT_PRICE")).alias("GROSS_AMOUNT"),
            
            (col("QUANTITY") * col("UNIT_PRICE") * col("DISCOUNT_PERCENT") / 100)
            .alias("DISCOUNT_AMOUNT"),
            
            (col("QUANTITY") * col("UNIT_PRICE") * (1 - col("DISCOUNT_PERCENT") / 100))
            .alias("NET_AMOUNT"),
            
            (col("QUANTITY") * col("UNIT_PRICE") * (1 - col("DISCOUNT_PERCENT") / 100) + col("TAX_AMOUNT"))
            .alias("TOTAL_AMOUNT"),
            
            (col("QUANTITY") * col("COST_PRICE")).alias("TOTAL_COST"),
            
            ((col("QUANTITY") * col("UNIT_PRICE") * (1 - col("DISCOUNT_PERCENT") / 100)) - 
             (col("QUANTITY") * col("COST_PRICE")))
            .alias("PROFIT_AMOUNT"),
            
            # Audit columns
            current_timestamp().alias("ETL_INSERT_DATE"),
            lit(self.workflow_name).alias("ETL_PROCESS_NAME")
        )
        
        return df_calculated
    
    def aggregator_transformation(self, df: Any) -> Any:
        """
        Replicate Informatica Aggregator transformation
        Aggregate sales by date, customer, and product
        """
        logger.info("Executing Aggregator transformation")
        
        agg_df = df.groupBy(
            "TRANSACTION_DATE",
            "CUSTOMER_ID",
            "PRODUCT_ID",
            "CATEGORY",
            "SUBCATEGORY"
        ).agg(
            count("TRANSACTION_ID").alias("TRANSACTION_COUNT"),
            sum("QUANTITY").alias("TOTAL_QUANTITY"),
            sum("GROSS_AMOUNT").alias("TOTAL_GROSS_AMOUNT"),
            sum("DISCOUNT_AMOUNT").alias("TOTAL_DISCOUNT_AMOUNT"),
            sum("NET_AMOUNT").alias("TOTAL_NET_AMOUNT"),
            sum("TAX_AMOUNT").alias("TOTAL_TAX_AMOUNT"),
            sum("TOTAL_AMOUNT").alias("TOTAL_SALES_AMOUNT"),
            sum("TOTAL_COST").alias("TOTAL_COST_AMOUNT"),
            sum("PROFIT_AMOUNT").alias("TOTAL_PROFIT_AMOUNT"),
            avg("UNIT_PRICE").alias("AVG_UNIT_PRICE"),
            max("UNIT_PRICE").alias("MAX_UNIT_PRICE"),
            min("UNIT_PRICE").alias("MIN_UNIT_PRICE")
        )
        
        # Add profit margin calculation
        agg_df = agg_df.withColumn(
            "PROFIT_MARGIN_PCT",
            when(col("TOTAL_SALES_AMOUNT") > 0,
                 (col("TOTAL_PROFIT_AMOUNT") / col("TOTAL_SALES_AMOUNT") * 100)
            ).otherwise(0)
        )
        
        return agg_df
    
    def update_strategy_transformation(self, df: Any) -> Any:
        """
        Replicate Informatica Update Strategy (SCD Type 1)
        Add update strategy flag
        """
        logger.info("Executing Update Strategy transformation")
        
        # Read existing target data
        try:
            existing_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config['target_jdbc_url']) \
                .option("dbtable", "FACT_SALES_DAILY") \
                .option("user", self.config['target_user']) \
                .option("password", self.config['target_password']) \
                .load()
            
            # Identify new and existing records
            df_with_strategy = df.join(
                existing_df,
                (df.TRANSACTION_DATE == existing_df.TRANSACTION_DATE) &
                (df.CUSTOMER_ID == existing_df.CUSTOMER_ID) &
                (df.PRODUCT_ID == existing_df.PRODUCT_ID),
                "left"
            ).select(
                df["*"],
                when(existing_df.TRANSACTION_DATE.isNull(), "INSERT")
                .otherwise("UPDATE").alias("UPDATE_STRATEGY")
            )
            
        except Exception as e:
            logger.warning(f"Target table not found, all records will be inserts: {str(e)}")
            df_with_strategy = df.withColumn("UPDATE_STRATEGY", lit("INSERT"))
        
        return df_with_strategy
    
    def execute_workflow(self) -> Dict[str, Any]:
        """Execute complete workflow"""
        logger.info(f"Starting execution of {self.workflow_name}")
        start_time = datetime.now()
        
        try:
            # Execute transformation pipeline
            sales_df = self.source_qualifier_sales()
            product_df = self.source_qualifier_product()
            joined_df = self.joiner_transformation(sales_df, product_df)
            calculated_df = self.expression_transformation(joined_df)
            aggregated_df = self.aggregator_transformation(calculated_df)
            final_df = self.update_strategy_transformation(aggregated_df)
            
            # Add audit columns
            final_df = final_df.withColumn("ETL_INSERT_DATE", current_timestamp()) \
                               .withColumn("ETL_UPDATE_DATE", current_timestamp()) \
                               .withColumn("ETL_PROCESS_NAME", lit(self.workflow_name))
            
            # Write to target (handle updates separately in production)
            inserts_df = final_df.filter(col("UPDATE_STRATEGY") == "INSERT")
            updates_df = final_df.filter(col("UPDATE_STRATEGY") == "UPDATE")
            
            if inserts_df.count() > 0:
                self.write_target_data(inserts_df.drop("UPDATE_STRATEGY"), 
                                     "FACT_SALES_DAILY", "append")
            
            if updates_df.count() > 0:
                # In production, use merge/upsert logic
                logger.info(f"Processing {updates_df.count()} updates")
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            result = {
                "workflow_name": self.workflow_name,
                "status": "SUCCESS",
                "records_processed": final_df.count(),
                "inserts": inserts_df.count(),
                "updates": updates_df.count(),
                "execution_time": execution_time,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Workflow completed successfully in {execution_time} seconds")
            return result
            
        except Exception as e:
            logger.error(f"Workflow execution failed: {str(e)}")
            raise


class DataValidationFramework:
    """
    Framework for validating data between source (Informatica) and target (PySpark)
    Ensures 100% data accuracy
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.validation_results = []
    
    def row_count_validation(self, workflow_name: str, 
                            source_df: Any, target_df: Any) -> ValidationResult:
        """Validate row counts match between source and target"""
        logger.info("Executing row count validation")
        start_time = datetime.now()
        
        source_count = source_df.count()
        target_count = target_df.count()
        match_percentage = (min(source_count, target_count) / max(source_count, target_count) * 100) \
                          if max(source_count, target_count) > 0 else 100.0
        
        status = ValidationStatus.PASSED if source_count == target_count else ValidationStatus.FAILED
        
        result = ValidationResult(
            workflow_name=workflow_name,
            validation_type="ROW_COUNT",
            status=status,
            source_count=source_count,
            target_count=target_count,
            match_percentage=match_percentage,
            discrepancies=[],
            execution_time=(datetime.now() - start_time).total_seconds(),
            timestamp=datetime.now().isoformat()
        )
        
        logger.info(f"Row count validation: {status.value} - Source: {source_count}, Target: {target_count}")
        return result
    
    def column_comparison_validation(self, workflow_name: str,
                                    source_df: Any, target_df: Any,
                                    key_columns: List[str],
                                    compare_columns: List[str]) -> ValidationResult:
        """Compare column values between source and target"""
        logger.info("Executing column comparison validation")
        start_time = datetime.now()
        
        discrepancies = []
        
        # Join source and target on key columns
        comparison_df = source_df.alias("src").join(
            target_df.alias("tgt"),
            key_columns,
            "full_outer"
        )
        
        # Check for null keys (missing records)
        for key_col in key_columns:
            missing_source = comparison_df.filter(col(f"src.{key_col}").isNull()).count()
            missing_target = comparison_df.filter(col(f"tgt.{key_col}").isNull()).count()
            
            if missing_source > 0:
                discrepancies.append({
                    "type": "MISSING_IN_SOURCE",
                    "column": key_col,
                    "count": missing_source
                })
            
            if missing_target > 0:
                discrepancies.append({
                    "type": "MISSING_IN_TARGET",
                    "column": key_col,
                    "count": missing_target
                })
        
        # Compare values for each column
        for col_name in compare_columns:
            mismatch_df = comparison_df.filter(
                col(f"src.{col_name}") != col(f"tgt.{col_name}")
            )
            mismatch_count = mismatch_df.count()
            
            if mismatch_count > 0:
                discrepancies.append({
                    "type": "VALUE_MISMATCH",
                    "column": col_name,
                    "count": mismatch_count
                })
        
        status = ValidationStatus.PASSED if len(discrepancies) == 0 else ValidationStatus.FAILED
        
        result = ValidationResult(
            workflow_name=workflow_name,
            validation_type="COLUMN_COMPARISON",
            status=status,
            source_count=source_df.count(),
            target_count=target_df.count(),
            match_percentage=100.0 if len(discrepancies) == 0 else 0.0,
            discrepancies=discrepancies,
            execution_time=(datetime.now() - start_time).total_seconds(),
            timestamp=datetime.now().isoformat()
        )
        
        logger.info(f"Column comparison validation: {status.value} - Discrepancies: {len(discrepancies)}")
        return result
    
    def checksum_validation(self, workflow_name: str,
                           source_df: Any, target_df: Any,
                           key_columns: List[str],
                           checksum_columns: List[str]) -> ValidationResult:
        """Validate using checksum for data integrity"""
        logger.info("Executing checksum validation")
        start_time = datetime.now()
        
        def calculate_checksum(df, columns):
            # Concatenate all columns and calculate MD5 hash
            concat_expr = concat_ws("|", *[coalesce(col(c).cast("string"), lit("NULL")) 
                                          for c in columns])
            return df.withColumn("CHECKSUM", md5(concat_expr))
        
        source_with_checksum = calculate_checksum(source_df, checksum_columns)
        target_with_checksum = calculate_checksum(target_df, checksum_columns)
        
        # Compare checksums
        comparison_df = source_with_checksum.alias("src").join(
            target_with_checksum.alias("tgt"),
            key_columns,
            "full_outer"
        )
        
        mismatches = comparison_df.filter(
            col("src.CHECKSUM") != col("tgt.CHECKSUM")
        ).count()
        
        total_records = max(source_df.count(), target_df.count())
        match_percentage = ((total_records - mismatches) / total_records * 100) if total_records > 0 else 100.0
        
        status = ValidationStatus.PASSED if mismatches == 0 else ValidationStatus.FAILED
        
        result = ValidationResult(
            workflow_name=workflow_name,
            validation_type="CHECKSUM",
            status=status,
            source_count=source_df.count(),
            target_count=target_df.count(),
            match_percentage=match_percentage,
            discrepancies=[{"type": "CHECKSUM_MISMATCH", "count": mismatches}] if mismatches > 0 else [],
            execution_time=(datetime.now() - start_time).total_seconds(),
            timestamp=datetime.now().isoformat()
        )
        
        logger.info(f"Checksum validation: {status.value} - Match percentage: {match_percentage}%")
        return result
    
    def aggregate_validation(self, workflow_name: str,
                            source_df: Any, target_df: Any,
                            group_columns: List[str],
                            agg_columns: List[str]) -> ValidationResult:
        """Validate aggregate values match"""
        logger.info("Executing aggregate validation")
        start_time = datetime.now()
        
        discrepancies = []
        
        # Calculate aggregates for source
        source_agg = source_df.groupBy(group_columns).agg(
            *[sum(col(c)).alias(f"{c}_SUM") for c in agg_columns],
            *[avg(col(c)).alias(f"{c}_AVG") for c in agg_columns],
            count("*").alias("RECORD_COUNT")
        )
        
        # Calculate aggregates for target
        target_agg = target_df.groupBy(group_columns).agg(
            *[sum(col(c)).alias(f"{c}_SUM") for c in agg_columns],
            *[avg(col(c)).alias(f"{c}_AVG") for c in agg_columns],
            count("*").alias("RECORD_COUNT")
        )
        
        # Compare aggregates
        comparison = source_agg.alias("src").join(
            target_agg.alias("tgt"),
            group_columns,
            "full_outer"
        )
        
        for agg_col in agg_columns:
            sum_mismatch = comparison.filter(
                abs(col(f"src.{agg_col}_SUM") - col(f"tgt.{agg_col}_SUM")) > 0.01
            ).count()
            
            if sum_mismatch > 0:
                discrepancies.append({
                    "type": "AGGREGATE_MISMATCH",
                    "column": f"{agg_col}_SUM",
                    "count": sum_mismatch
                })
        
        status = ValidationStatus.PASSED if len(discrepancies) == 0 else ValidationStatus.FAILED
        
        result = ValidationResult(
            workflow_name=workflow_name,
            validation_type="AGGREGATE",
            status=status,
            source_count=source_df.count(),
            target_count=target_df.count(),
            match_percentage=100.0 if len(discrepancies) == 0 else 0.0,
            discrepancies=discrepancies,
            execution_time=(datetime.now() - start_time).total_seconds(),
            timestamp=datetime.now().isoformat()
        )
        
        logger.info(f"Aggregate validation: {status.value}")
        return result


class PerformanceTestingFramework:
    """
    Framework for performance testing and comparison
    Compares PySpark performance against Informatica baseline
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.performance_metrics = []
    
    def measure_execution_time(self, workflow_name: