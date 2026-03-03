import logging
import time
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from functools import wraps
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import yaml
from pathlib import Path


class SparkConnectionManager:
    """
    Manages Spark session connections with support for different environments
    and connection pooling
    """
    
    _instance = None
    _spark_session = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkConnectionManager, cls).__new__(cls)
        return cls._instance
    
    @classmethod
    def get_spark_session(cls, app_name: str = "DataMigration", 
                         config: Dict[str, str] = None) -> SparkSession:
        """
        Get or create Spark session with connection pooling
        
        Args:
            app_name: Application name for Spark session
            config: Additional Spark configurations
            
        Returns:
            SparkSession instance
        """
        if cls._spark_session is None:
            builder = SparkSession.builder.appName(app_name)
            
            default_config = {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.shuffle.partitions": "200",
                "spark.sql.sources.partitionOverwriteMode": "dynamic",
                "spark.sql.legacy.timeParserPolicy": "LEGACY",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
            }
            
            if config:
                default_config.update(config)
            
            for key, value in default_config.items():
                builder = builder.config(key, value)
            
            cls._spark_session = builder.getOrCreate()
            cls._spark_session.sparkContext.setLogLevel("WARN")
            
        return cls._spark_session
    
    @classmethod
    def close_session(cls):
        """Close the active Spark session"""
        if cls._spark_session:
            cls._spark_session.stop()
            cls._spark_session = None


class ConfigurationManager:
    """
    Manages application configuration across multiple environments
    """
    
    def __init__(self, config_path: str, environment: str = "dev"):
        """
        Initialize configuration manager
        
        Args:
            config_path: Path to configuration file (YAML/JSON)
            environment: Target environment (dev/test/prod)
        """
        self.config_path = Path(config_path)
        self.environment = environment
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            if self.config_path.suffix in ['.yaml', '.yml']:
                all_config = yaml.safe_load(f)
            elif self.config_path.suffix == '.json':
                all_config = json.load(f)
            else:
                raise ValueError(f"Unsupported config format: {self.config_path.suffix}")
        
        return all_config.get(self.environment, {})
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key
        
        Args:
            key: Configuration key (supports dot notation for nested keys)
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    def get_connection_string(self, source: str) -> str:
        """Get database connection string for specified source"""
        return self.get(f'connections.{source}.connection_string')
    
    def get_table_config(self, table_name: str) -> Dict[str, Any]:
        """Get configuration for specific table"""
        return self.get(f'tables.{table_name}', {})


class LoggingManager:
    """
    Centralized logging and error handling framework
    """
    
    def __init__(self, log_name: str, log_level: str = "INFO",
                 log_path: Optional[str] = None):
        """
        Initialize logging manager
        
        Args:
            log_name: Logger name
            log_level: Logging level
            log_path: Optional file path for logs
        """
        self.logger = logging.getLogger(log_name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        if log_path:
            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        self.job_metrics = {}
    
    def log_job_start(self, job_name: str, parameters: Dict[str, Any] = None):
        """Log job start with parameters"""
        self.job_metrics[job_name] = {
            'job_name': job_name,
            'start_time': datetime.now(),
            'parameters': parameters or {},
            'status': 'RUNNING'
        }
        self.logger.info(f"Job started: {job_name} with parameters: {parameters}")
    
    def log_job_end(self, job_name: str, status: str = "SUCCESS", 
                    metrics: Dict[str, Any] = None):
        """Log job completion with metrics"""
        if job_name in self.job_metrics:
            end_time = datetime.now()
            start_time = self.job_metrics[job_name]['start_time']
            duration = (end_time - start_time).total_seconds()
            
            self.job_metrics[job_name].update({
                'end_time': end_time,
                'duration_seconds': duration,
                'status': status,
                'metrics': metrics or {}
            })
            
            self.logger.info(
                f"Job completed: {job_name} | Status: {status} | "
                f"Duration: {duration:.2f}s | Metrics: {metrics}"
            )
    
    def log_dataframe_info(self, df: DataFrame, df_name: str):
        """Log DataFrame information"""
        count = df.count()
        self.logger.info(f"DataFrame: {df_name} | Row count: {count}")
        self.logger.debug(f"Schema: {df.schema}")
    
    def get_job_metrics(self, job_name: str) -> Dict[str, Any]:
        """Retrieve metrics for specific job"""
        return self.job_metrics.get(job_name, {})
    
    def export_metrics(self, output_path: str):
        """Export all job metrics to JSON file"""
        metrics_copy = {}
        for job_name, metrics in self.job_metrics.items():
            metrics_copy[job_name] = {
                k: str(v) if isinstance(v, datetime) else v
                for k, v in metrics.items()
            }
        
        with open(output_path, 'w') as f:
            json.dump(metrics_copy, f, indent=2)


def log_execution_time(logger: LoggingManager):
    """
    Decorator to log function execution time
    
    Args:
        logger: LoggingManager instance
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger.logger.info(f"Starting execution: {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.logger.info(
                    f"Completed execution: {func.__name__} in {execution_time:.2f}s"
                )
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.logger.error(
                    f"Failed execution: {func.__name__} after {execution_time:.2f}s - {str(e)}"
                )
                raise
        
        return wrapper
    return decorator


class DataQualityValidator:
    """
    Reusable data quality validation functions
    """
    
    def __init__(self, spark: SparkSession, logger: LoggingManager):
        """
        Initialize data quality validator
        
        Args:
            spark: SparkSession instance
            logger: LoggingManager instance
        """
        self.spark = spark
        self.logger = logger
    
    def check_null_values(self, df: DataFrame, columns: List[str],
                         threshold: float = 0.0) -> Dict[str, Any]:
        """
        Check for null values in specified columns
        
        Args:
            df: Input DataFrame
            columns: List of columns to check
            threshold: Maximum allowed null percentage (0.0 to 1.0)
            
        Returns:
            Dictionary with validation results
        """
        total_count = df.count()
        results = {
            'check': 'null_values',
            'passed': True,
            'columns': {}
        }
        
        for col in columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_percentage = null_count / total_count if total_count > 0 else 0
            
            passed = null_percentage <= threshold
            results['columns'][col] = {
                'null_count': null_count,
                'null_percentage': null_percentage,
                'threshold': threshold,
                'passed': passed
            }
            
            if not passed:
                results['passed'] = False
                self.logger.logger.warning(
                    f"Null check failed for {col}: {null_percentage:.2%} > {threshold:.2%}"
                )
        
        return results
    
    def check_duplicates(self, df: DataFrame, key_columns: List[str]) -> Dict[str, Any]:
        """
        Check for duplicate records based on key columns
        
        Args:
            df: Input DataFrame
            key_columns: List of columns that define uniqueness
            
        Returns:
            Dictionary with validation results
        """
        total_count = df.count()
        distinct_count = df.select(key_columns).distinct().count()
        duplicate_count = total_count - distinct_count
        
        results = {
            'check': 'duplicates',
            'total_count': total_count,
            'distinct_count': distinct_count,
            'duplicate_count': duplicate_count,
            'passed': duplicate_count == 0
        }
        
        if duplicate_count > 0:
            self.logger.logger.warning(
                f"Duplicate check failed: {duplicate_count} duplicate records found"
            )
        
        return results
    
    def check_data_types(self, df: DataFrame, 
                        expected_schema: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate DataFrame schema against expected data types
        
        Args:
            df: Input DataFrame
            expected_schema: Dictionary of column_name -> expected_type
            
        Returns:
            Dictionary with validation results
        """
        results = {
            'check': 'data_types',
            'passed': True,
            'columns': {}
        }
        
        for field in df.schema.fields:
            col_name = field.name
            actual_type = str(field.dataType)
            
            if col_name in expected_schema:
                expected_type = expected_schema[col_name]
                passed = expected_type.lower() in actual_type.lower()
                
                results['columns'][col_name] = {
                    'expected': expected_type,
                    'actual': actual_type,
                    'passed': passed
                }
                
                if not passed:
                    results['passed'] = False
                    self.logger.logger.warning(
                        f"Data type mismatch for {col_name}: "
                        f"expected {expected_type}, got {actual_type}"
                    )
        
        return results
    
    def check_value_range(self, df: DataFrame, column: str,
                         min_value: Any = None, max_value: Any = None) -> Dict[str, Any]:
        """
        Check if values in column are within specified range
        
        Args:
            df: Input DataFrame
            column: Column name to check
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            
        Returns:
            Dictionary with validation results
        """
        stats = df.select(
            F.min(column).alias('min'),
            F.max(column).alias('max')
        ).collect()[0]
        
        actual_min = stats['min']
        actual_max = stats['max']
        
        passed = True
        violations = []
        
        if min_value is not None and actual_min < min_value:
            passed = False
            violations.append(f"Min value {actual_min} < threshold {min_value}")
        
        if max_value is not None and actual_max > max_value:
            passed = False
            violations.append(f"Max value {actual_max} > threshold {max_value}")
        
        results = {
            'check': 'value_range',
            'column': column,
            'actual_min': actual_min,
            'actual_max': actual_max,
            'expected_min': min_value,
            'expected_max': max_value,
            'passed': passed,
            'violations': violations
        }
        
        if not passed:
            self.logger.logger.warning(
                f"Value range check failed for {column}: {violations}"
            )
        
        return results
    
    def check_referential_integrity(self, df_child: DataFrame, df_parent: DataFrame,
                                   child_key: str, parent_key: str) -> Dict[str, Any]:
        """
        Check referential integrity between child and parent tables
        
        Args:
            df_child: Child DataFrame
            df_parent: Parent DataFrame
            child_key: Foreign key column in child table
            parent_key: Primary key column in parent table
            
        Returns:
            Dictionary with validation results
        """
        orphan_records = df_child.join(
            df_parent,
            df_child[child_key] == df_parent[parent_key],
            'left_anti'
        ).filter(F.col(child_key).isNotNull())
        
        orphan_count = orphan_records.count()
        
        results = {
            'check': 'referential_integrity',
            'child_key': child_key,
            'parent_key': parent_key,
            'orphan_count': orphan_count,
            'passed': orphan_count == 0
        }
        
        if orphan_count > 0:
            self.logger.logger.warning(
                f"Referential integrity check failed: {orphan_count} orphan records found"
            )
        
        return results
    
    def check_custom_rule(self, df: DataFrame, rule_name: str,
                         condition: str) -> Dict[str, Any]:
        """
        Apply custom validation rule using SQL condition
        
        Args:
            df: Input DataFrame
            rule_name: Name of the validation rule
            condition: SQL condition to evaluate (returns True for valid records)
            
        Returns:
            Dictionary with validation results
        """
        total_count = df.count()
        valid_count = df.filter(condition).count()
        invalid_count = total_count - valid_count
        
        results = {
            'check': 'custom_rule',
            'rule_name': rule_name,
            'condition': condition,
            'total_count': total_count,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'passed': invalid_count == 0
        }
        
        if invalid_count > 0:
            self.logger.logger.warning(
                f"Custom rule '{rule_name}' failed: {invalid_count} invalid records"
            )
        
        return results
    
    def run_all_checks(self, df: DataFrame, 
                      validation_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run all configured validation checks
        
        Args:
            df: Input DataFrame
            validation_config: Dictionary with validation configuration
            
        Returns:
            Dictionary with all validation results
        """
        all_results = {
            'timestamp': datetime.now().isoformat(),
            'checks': [],
            'overall_passed': True
        }
        
        if 'null_checks' in validation_config:
            for check in validation_config['null_checks']:
                result = self.check_null_values(
                    df, check['columns'], check.get('threshold', 0.0)
                )
                all_results['checks'].append(result)
                if not result['passed']:
                    all_results['overall_passed'] = False
        
        if 'duplicate_checks' in validation_config:
            for check in validation_config['duplicate_checks']:
                result = self.check_duplicates(df, check['key_columns'])
                all_results['checks'].append(result)
                if not result['passed']:
                    all_results['overall_passed'] = False
        
        if 'schema_checks' in validation_config:
            result = self.check_data_types(df, validation_config['schema_checks'])
            all_results['checks'].append(result)
            if not result['passed']:
                all_results['overall_passed'] = False
        
        if 'range_checks' in validation_config:
            for check in validation_config['range_checks']:
                result = self.check_value_range(
                    df, check['column'], 
                    check.get('min_value'), 
                    check.get('max_value')
                )
                all_results['checks'].append(result)
                if not result['passed']:
                    all_results['overall_passed'] = False
        
        if 'custom_rules' in validation_config:
            for check in validation_config['custom_rules']:
                result = self.check_custom_rule(
                    df, check['name'], check['condition']
                )
                all_results['checks'].append(result)
                if not result['passed']:
                    all_results['overall_passed'] = False
        
        return all_results


class TransformationUtils:
    """
    Reusable PySpark transformation utility functions
    """
    
    @staticmethod
    def standardize_column_names(df: DataFrame, 
                                 case: str = "lower",
                                 replace_spaces: str = "_") -> DataFrame:
        """
        Standardize column names (case, spaces, special characters)
        
        Args:
            df: Input DataFrame
            case: Target case (lower/upper/title)
            replace_spaces: Character to replace spaces
            
        Returns:
            DataFrame with standardized column names
        """
        for col in df.columns:
            new_col = col.replace(" ", replace_spaces)
            new_col = new_col.replace("-", replace_spaces)
            
            if case == "lower":
                new_col = new_col.lower()
            elif case == "upper":
                new_col = new_col.upper()
            elif case == "title":
                new_col = new_col.title()
            
            df = df.withColumnRenamed(col, new_col)
        
        return df
    
    @staticmethod
    def add_audit_columns(df: DataFrame, 
                         load_date: datetime = None,
                         source_system: str = None) -> DataFrame:
        """
        Add standard audit columns to DataFrame
        
        Args:
            df: Input DataFrame
            load_date: Load timestamp (defaults to current)
            source_system: Source system identifier
            
        Returns:
            DataFrame with audit columns
        """
        if load_date is None:
            load_date = datetime.now()
        
        df = df.withColumn("load_timestamp", F.lit(load_date))
        df = df.withColumn("load_date", F.lit(load_date.date()))
        
        if source_system:
            df = df.withColumn("source_system", F.lit(source_system))
        
        df = df.withColumn("record_hash", 
                          F.sha2(F.concat_ws("||", *df.columns), 256))
        
        return df
    
    @staticmethod
    def apply_scd_type2(df_source: DataFrame, df_target: DataFrame,
                       key_columns: List[str], 
                       compare_columns: List[str]) -> DataFrame:
        """
        Apply SCD Type 2 logic to track historical changes
        
        Args:
            df_source: Source DataFrame with new data
            df_target: Target DataFrame with existing data
            key_columns: Business key columns
            compare_columns: Columns to compare for changes
            
        Returns:
            DataFrame with SCD Type 2 logic applied
        """
        df_source = df_source.withColumn("is_current", F.lit(True))
        df_source = df_source.withColumn("effective_date", F.current_timestamp())
        df_source = df_source.withColumn("end_date", F.lit(None).cast(TimestampType()))
        
        if df_target.count() == 0:
            return df_source
        
        df_source_hash = df_source.withColumn(
            "source_hash",
            F.sha2(F.concat_ws("||", *compare_columns), 256)
        )
        
        df_target_hash = df_target.withColumn(
            "target_hash",
            F.sha2(F.concat_ws("||", *compare_columns), 256)
        )
        
        df_changed = df_source_hash.alias("src").join(
            df_target_hash.alias("tgt"),
            key_columns,
            "inner"
        ).where(F.col("src.source_hash") != F.col("tgt.target_hash"))
        
        df_target_expired = df_target.alias("tgt").join(
            df_changed.select([F.col("src." + c) for c in key_columns]),
            key_columns,
            "inner"
        ).withColumn("is_current", F.lit(False)).withColumn(
            "end_date", F.current_timestamp()
        )
        
        df_unchanged = df_target.alias("tgt").join(
            df_changed.select([F.col("src." + c) for c in key_columns]),
            key_columns,
            "left_anti"
        )
        
        df_new = df_source.alias("src").join(
            df_target.select(key_columns),
            key_columns,
            "left_anti"
        )
        
        result = df_target_expired.unionByName(df_unchanged).unionByName(df_new)
        
        return result
    
    @staticmethod
    def remove_duplicates(df: DataFrame, 
                         key_columns: List[str],
                         order_column: str = None,
                         ascending: bool = False) -> DataFrame:
        """
        Remove duplicate records keeping the first/last occurrence
        
        Args:
            df: Input DataFrame
            key_columns: Columns to identify duplicates
            order_column: Column to order by when selecting which record to keep
            ascending: Sort order for order_column
            
        Returns:
            DataFrame with duplicates removed
        """
        if order_column:
            window_spec = Window.partitionBy(key_columns).orderBy(
                F.col(order_column).asc() if ascending else F.col(order_column).desc()
            )
            df = df.withColumn("row_num", F.row_number().over(window_spec))
            df = df.filter(F.col("row_num") == 1).drop("row_num")
        else:
            df = df.dropDuplicates(key_columns)
        
        return df
    
    @staticmethod
    def pivot_data(df: DataFrame, 
                  group_columns: List[str],
                  pivot_column: str,
                  agg_column: str,
                  agg_func: str = "sum") -> DataFrame:
        """
        Pivot DataFrame from long to wide format
        
        Args:
            df: Input DataFrame
            group_columns: Columns to group by
            pivot_column: Column to pivot on
            agg_column: Column to aggregate
            agg_func: Aggregation function (sum/avg/max/min/count)
            
        Returns:
            Pivoted DataFrame
        """
        agg_functions = {
            "sum": F.sum,
            "avg": F.avg,
            "max": F.max,
            "min": F.min,
            "count": F.count
        }
        
        agg_fn = agg_functions.get(agg_func.lower(), F.sum)
        
        pivoted_df = df.groupBy(group_columns).pivot(pivot_column).agg(
            agg_fn(agg_column)
        )
        
        return pivoted_df
    
    @staticmethod
    def unpivot_data(df: DataFrame,
                    id_columns: List[str],
                    value_columns: List[str],
                    var_name: str = "variable",
                    value_name: str = "value") -> DataFrame:
        """
        Unpivot DataFrame from wide to long format
        
        Args:
            df: Input DataFrame
            id_columns: Columns to keep as identifiers
            value_columns: Columns to unpivot
            var_name: Name for the variable column
            value_name: Name for the value column
            
        Returns:
            Unpivoted DataFrame
        """
        unpivot_expr = f"stack({len(value_columns)}, "
        unpivot_expr += ", ".join([f"'{col}', {col}" for col in value_columns])
        unpivot_expr += f") as ({var_name}, {value_name})"
        
        unpivoted_df = df.select(
            *id_columns,
            F.expr(unpivot_expr)
        )
        
        return unpivoted_df
    
    @staticmethod
    def apply_business_rules(df: DataFrame, 
                           rules: List[Dict[str, str]]) -> DataFrame:
        """
        Apply business rules to create derived columns
        
        Args:
            df: Input DataFrame
            rules: List of dicts with 'column' and 'expression' keys
            
        Returns:
            DataFrame with derived columns
        """
        for rule in rules:
            df = df.withColumn(rule['column'], F.expr(rule['expression']))
        
        return df
    
    @staticmethod
    def handle_null_values(df: DataFrame,
                          strategy: str = "drop",
                          fill_values: Dict[str, Any] = None,
                          columns: List[str] = None) -> DataFrame:
        """
        Handle null values using specified strategy
        
        Args:
            df: Input DataFrame
            strategy: Strategy to handle nulls (drop/fill/forward_fill)
            fill_values: Dictionary of column -> fill_value for fill strategy
            columns: Specific columns to apply strategy to
            
        Returns:
            DataFrame with nulls handled
        """
        if strategy == "drop":
            if columns:
                df = df.dropna(subset=columns)
            else:
                df = df.dropna()
        
        elif strategy == "fill":
            if fill_values:
                df = df.fillna(fill_values)
            else:
                numeric_cols = [f.name for f in df.schema.fields 
                              if isinstance(f.dataType, (IntegerType, LongType, 
                                                        DoubleType, FloatType))]
                string_cols = [f.name for f in df.schema.fields 
                             if isinstance(f.dataType, StringType)]
                
                if numeric_cols:
                    df = df.fillna(0, subset=numeric_cols)
                if string_cols:
                    df = df.fillna("", subset=string_cols)
        
        elif strategy == "forward_fill":
            if columns:
                window_spec = Window.orderBy(F.monotonically_increasing_id())
                for col in columns:
                    df = df.withColumn(
                        col,
                        F.last(col, ignorenulls=True).over(window_spec)
                    )
        
        return df
    
    @staticmethod
    def split_column(df: DataFrame, 
                    column: str,
                    delimiter: str,
                    new_columns: List[str]) -> DataFrame:
        """
        Split a column into multiple columns
        
        Args:
            df: Input DataFrame
            column: Column to split
            delimiter: Split delimiter
            new_columns: Names for new columns
            
        Returns:
            DataFrame with split columns
        """
        split_col = F.split(F.col(column), delimiter)
        
        for i, new_col in enumerate(new_columns):
            df = df.withColumn(new_col, split_col.getItem(i))
        
        return df
    
    @staticmethod
    def aggregate_with_window(df: DataFrame,
                             partition_columns: List[str],
                             order_columns: List[str],
                             agg_expressions: Dict[str, str]) -> DataFrame:
        """
        Apply window aggregations
        
        Args:
            df: Input DataFrame
            partition_columns: Columns to partition by
            order_columns: Columns to order by
            agg_expressions: Dict of new_column -> aggregation expression
            
        Returns:
            DataFrame with window aggregations
        """
        window_spec = Window.partitionBy(partition_columns).orderBy(order_columns)
        
        for new_col, expr in agg_expressions.items():
            df = df.withColumn(new_col, F.expr(expr).over(window_spec))
        
        return df


class DataLineageTracker:
    """
    Track data lineage and transformations
    """
    
    def __init__(self, job_name: str):
        """
        Initialize lineage tracker
        
        Args:
            job_name: Name of the job/pipeline
        """
        self.job_name = job_name
        self.lineage = {
            'job_name': job_name,
            'start_time': datetime.now().isoformat(),
            'transformations': []
        }
    
    def track_read(self, source: str, table: str, 
                   filters: str = None, row_count: int = None):
        """Track data read operation"""
        self.lineage['transformations'].append({
            'operation': 'READ',
            'timestamp': datetime.now().isoformat(),
            'source': source,
            'table': table,
            'filters': filters,
            'row_count': row_count
        })
    
    def track_transformation(self, operation: str, 
                           description: str,
                           input_count: int = None,
                           output_count: int = None):
        """Track transformation operation"""
        self.lineage['transformations'].append({
            'operation': operation,
            'timestamp': datetime.now().isoformat(),
            'description': description,
            'input_count': input_count,
            'output_count': output_count
        })
    
    def track_write(self, target: str, table: str, 
                   mode: str, row_count: int = None):
        """Track data write operation"""
        self.lineage['transformations'].append({