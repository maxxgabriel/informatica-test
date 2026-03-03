import logging
import functools
import time
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Callable, Optional, Union
from dataclasses import dataclass, asdict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.utils import AnalysisException
import yaml


# ============================================================================
# CONFIGURATION MANAGEMENT MODULE
# ============================================================================

class ConfigurationManager:
    """
    Manages application configuration across multiple environments.
    Supports YAML, JSON, and environment variable overrides.
    """
    
    def __init__(self, config_path: str, environment: str = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file
            environment: Target environment (dev, test, prod)
        """
        self.config_path = config_path
        self.environment = environment or os.getenv('ENVIRONMENT', 'dev')
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        try:
            with open(self.config_path, 'r') as f:
                if self.config_path.endswith('.yaml') or self.config_path.endswith('.yml'):
                    all_config = yaml.safe_load(f)
                elif self.config_path.endswith('.json'):
                    all_config = json.load(f)
                else:
                    raise ValueError(f"Unsupported config format: {self.config_path}")
            
            # Get environment-specific config
            config = all_config.get('common', {})
            env_config = all_config.get(self.environment, {})
            config.update(env_config)
            
            # Override with environment variables
            config = self._apply_env_overrides(config)
            
            return config
        except Exception as e:
            raise RuntimeError(f"Failed to load configuration: {str(e)}")
    
    def _apply_env_overrides(self, config: Dict) -> Dict:
        """Apply environment variable overrides."""
        for key, value in config.items():
            env_key = f"APP_{key.upper()}"
            if env_key in os.environ:
                config[key] = os.environ[env_key]
        return config
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value
    
    def get_connection_config(self, conn_name: str) -> Dict[str, Any]:
        """Get connection configuration."""
        connections = self.get('connections', {})
        return connections.get(conn_name, {})


# ============================================================================
# LOGGING AND ERROR HANDLING FRAMEWORK
# ============================================================================

class JobLogger:
    """
    Centralized logging framework with metrics capture.
    """
    
    def __init__(self, job_name: str, log_level: str = 'INFO'):
        """
        Initialize job logger.
        
        Args:
            job_name: Name of the job
            log_level: Logging level
        """
        self.job_name = job_name
        self.job_id = f"{job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.start_time = datetime.now()
        self.metrics = {
            'job_name': job_name,
            'job_id': self.job_id,
            'start_time': self.start_time.isoformat(),
            'status': 'RUNNING'
        }
        
        # Configure logger
        self.logger = logging.getLogger(job_name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        self.logger.info(f"Job {self.job_name} started with ID: {self.job_id}")
    
    def log_metric(self, metric_name: str, metric_value: Any):
        """Log a metric."""
        self.metrics[metric_name] = metric_value
        self.logger.info(f"Metric - {metric_name}: {metric_value}")
    
    def log_dataframe_info(self, df: DataFrame, df_name: str):
        """Log DataFrame information."""
        count = df.count()
        columns = len(df.columns)
        self.log_metric(f"{df_name}_count", count)
        self.log_metric(f"{df_name}_columns", columns)
        self.logger.info(f"DataFrame {df_name}: {count} rows, {columns} columns")
    
    def finalize(self, status: str = 'SUCCESS'):
        """Finalize job logging."""
        self.metrics['status'] = status
        self.metrics['end_time'] = datetime.now().isoformat()
        duration = (datetime.now() - self.start_time).total_seconds()
        self.metrics['duration_seconds'] = duration
        
        self.logger.info(f"Job {self.job_name} completed with status: {status}")
        self.logger.info(f"Job duration: {duration:.2f} seconds")
        
        return self.metrics


class ErrorHandler:
    """
    Centralized error handling with retry logic.
    """
    
    @staticmethod
    def retry(max_attempts: int = 3, delay: int = 5, exceptions: tuple = (Exception,)):
        """
        Decorator for retry logic.
        
        Args:
            max_attempts: Maximum retry attempts
            delay: Delay between retries in seconds
            exceptions: Tuple of exceptions to catch
        """
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                attempt = 0
                while attempt < max_attempts:
                    try:
                        return func(*args, **kwargs)
                    except exceptions as e:
                        attempt += 1
                        if attempt >= max_attempts:
                            raise
                        logging.warning(
                            f"Attempt {attempt} failed for {func.__name__}: {str(e)}. "
                            f"Retrying in {delay} seconds..."
                        )
                        time.sleep(delay)
            return wrapper
        return decorator
    
    @staticmethod
    def safe_execute(func: Callable, default_value: Any = None, logger: logging.Logger = None):
        """
        Safely execute a function with error handling.
        
        Args:
            func: Function to execute
            default_value: Default value on error
            logger: Logger instance
        """
        try:
            return func()
        except Exception as e:
            if logger:
                logger.error(f"Error executing {func.__name__}: {str(e)}")
            return default_value


# ============================================================================
# CONNECTION MANAGEMENT FRAMEWORK
# ============================================================================

class ConnectionPool:
    """
    Manages database and service connections with pooling.
    """
    
    def __init__(self, config_manager: ConfigurationManager):
        """
        Initialize connection pool.
        
        Args:
            config_manager: Configuration manager instance
        """
        self.config_manager = config_manager
        self._connections = {}
    
    def get_jdbc_properties(self, conn_name: str) -> Dict[str, str]:
        """Get JDBC connection properties."""
        conn_config = self.config_manager.get_connection_config(conn_name)
        
        return {
            'user': conn_config.get('user'),
            'password': conn_config.get('password'),
            'driver': conn_config.get('driver'),
            'fetchsize': str(conn_config.get('fetchsize', 10000)),
            'numPartitions': str(conn_config.get('num_partitions', 4))
        }
    
    def get_jdbc_url(self, conn_name: str) -> str:
        """Get JDBC URL."""
        conn_config = self.config_manager.get_connection_config(conn_name)
        
        db_type = conn_config.get('type', 'postgresql')
        host = conn_config.get('host')
        port = conn_config.get('port')
        database = conn_config.get('database')
        
        if db_type == 'postgresql':
            return f"jdbc:postgresql://{host}:{port}/{database}"
        elif db_type == 'oracle':
            return f"jdbc:oracle:thin:@{host}:{port}:{database}"
        elif db_type == 'sqlserver':
            return f"jdbc:sqlserver://{host}:{port};databaseName={database}"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def read_jdbc(self, spark: SparkSession, conn_name: str, query: str = None, 
                  table: str = None, partition_column: str = None,
                  lower_bound: int = None, upper_bound: int = None,
                  num_partitions: int = None) -> DataFrame:
        """
        Read data from JDBC source.
        
        Args:
            spark: SparkSession
            conn_name: Connection name
            query: SQL query
            table: Table name
            partition_column: Column for partitioning
            lower_bound: Lower bound for partitioning
            upper_bound: Upper bound for partitioning
            num_partitions: Number of partitions
        """
        jdbc_url = self.get_jdbc_url(conn_name)
        jdbc_props = self.get_jdbc_properties(conn_name)
        
        reader = spark.read.format('jdbc').option('url', jdbc_url)
        
        for key, value in jdbc_props.items():
            if value:
                reader = reader.option(key, value)
        
        if query:
            reader = reader.option('query', query)
        elif table:
            reader = reader.option('dbtable', table)
        else:
            raise ValueError("Either query or table must be provided")
        
        if partition_column and lower_bound is not None and upper_bound is not None:
            reader = reader.option('partitionColumn', partition_column) \
                           .option('lowerBound', lower_bound) \
                           .option('upperBound', upper_bound) \
                           .option('numPartitions', num_partitions or 4)
        
        return reader.load()
    
    def write_jdbc(self, df: DataFrame, conn_name: str, table: str, 
                   mode: str = 'append', batch_size: int = 10000):
        """
        Write data to JDBC destination.
        
        Args:
            df: DataFrame to write
            conn_name: Connection name
            table: Target table
            mode: Write mode (append, overwrite, etc.)
            batch_size: Batch size for writing
        """
        jdbc_url = self.get_jdbc_url(conn_name)
        jdbc_props = self.get_jdbc_properties(conn_name)
        jdbc_props['batchsize'] = str(batch_size)
        
        df.write.format('jdbc') \
          .option('url', jdbc_url) \
          .options(**jdbc_props) \
          .option('dbtable', table) \
          .mode(mode) \
          .save()


# ============================================================================
# PYSPARK UTILITY LIBRARY
# ============================================================================

class SparkUtils:
    """
    Common PySpark transformation utilities.
    """
    
    @staticmethod
    def standardize_column_names(df: DataFrame, case: str = 'lower', 
                                 replace_spaces: str = '_') -> DataFrame:
        """
        Standardize column names.
        
        Args:
            df: Input DataFrame
            case: Case conversion (lower, upper, title)
            replace_spaces: Character to replace spaces
        """
        new_columns = []
        for col in df.columns:
            new_col = col.replace(' ', replace_spaces)
            if case == 'lower':
                new_col = new_col.lower()
            elif case == 'upper':
                new_col = new_col.upper()
            elif case == 'title':
                new_col = new_col.title()
            new_columns.append(new_col)
        
        for old_col, new_col in zip(df.columns, new_columns):
            df = df.withColumnRenamed(old_col, new_col)
        
        return df
    
    @staticmethod
    def add_audit_columns(df: DataFrame, job_id: str = None) -> DataFrame:
        """
        Add audit columns to DataFrame.
        
        Args:
            df: Input DataFrame
            job_id: Job identifier
        """
        df = df.withColumn('insert_timestamp', F.current_timestamp()) \
               .withColumn('update_timestamp', F.current_timestamp()) \
               .withColumn('job_id', F.lit(job_id or 'unknown'))
        
        return df
    
    @staticmethod
    def apply_scd_type2(df_source: DataFrame, df_target: DataFrame,
                       business_keys: List[str], compare_columns: List[str],
                       effective_date_col: str = 'effective_date',
                       end_date_col: str = 'end_date',
                       current_flag_col: str = 'is_current') -> DataFrame:
        """
        Apply SCD Type 2 logic.
        
        Args:
            df_source: Source DataFrame
            df_target: Target DataFrame
            business_keys: List of business key columns
            compare_columns: Columns to compare for changes
            effective_date_col: Effective date column name
            end_date_col: End date column name
            current_flag_col: Current flag column name
        """
        current_date = F.current_date()
        max_date = F.lit('9999-12-31').cast('date')
        
        # Add metadata columns to source
        df_source = df_source.withColumn(effective_date_col, current_date) \
                             .withColumn(end_date_col, max_date) \
                             .withColumn(current_flag_col, F.lit(True))
        
        # Filter current records from target
        df_target_current = df_target.filter(F.col(current_flag_col) == True)
        
        # Join source and target on business keys
        join_condition = [df_source[key] == df_target_current[key] for key in business_keys]
        df_joined = df_source.alias('src').join(
            df_target_current.alias('tgt'),
            join_condition,
            'left'
        )
        
        # Identify changed records
        change_conditions = [
            F.coalesce(F.col(f'src.{col}'), F.lit('')) != F.coalesce(F.col(f'tgt.{col}'), F.lit(''))
            for col in compare_columns
        ]
        change_condition = functools.reduce(lambda a, b: a | b, change_conditions)
        
        # New records (not in target)
        df_new = df_joined.filter(F.col(f'tgt.{business_keys[0]}').isNull()) \
                          .select('src.*')
        
        # Unchanged records
        df_unchanged = df_joined.filter(
            F.col(f'tgt.{business_keys[0]}').isNotNull() & ~change_condition
        ).select('tgt.*')
        
        # Changed records - expire old
        df_expired = df_joined.filter(
            F.col(f'tgt.{business_keys[0]}').isNotNull() & change_condition
        ).select('tgt.*') \
         .withColumn(end_date_col, F.date_sub(current_date, 1)) \
         .withColumn(current_flag_col, F.lit(False))
        
        # Changed records - insert new
        df_changed_new = df_joined.filter(
            F.col(f'tgt.{business_keys[0]}').isNotNull() & change_condition
        ).select('src.*')
        
        # Combine all records
        df_result = df_new.union(df_unchanged).union(df_expired).union(df_changed_new)
        
        return df_result
    
    @staticmethod
    def deduplicate(df: DataFrame, partition_cols: List[str], 
                   order_cols: List[str], order_desc: bool = True) -> DataFrame:
        """
        Deduplicate DataFrame using window function.
        
        Args:
            df: Input DataFrame
            partition_cols: Columns to partition by
            order_cols: Columns to order by
            order_desc: Order descending
        """
        from pyspark.sql.window import Window
        
        order_exprs = [F.col(c).desc() if order_desc else F.col(c).asc() 
                      for c in order_cols]
        
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_exprs)
        
        df_dedup = df.withColumn('row_num', F.row_number().over(window_spec)) \
                     .filter(F.col('row_num') == 1) \
                     .drop('row_num')
        
        return df_dedup
    
    @staticmethod
    def pivot_wide_to_long(df: DataFrame, id_cols: List[str], 
                          value_cols: List[str], var_name: str = 'variable',
                          value_name: str = 'value') -> DataFrame:
        """
        Pivot DataFrame from wide to long format.
        
        Args:
            df: Input DataFrame
            id_cols: ID columns to keep
            value_cols: Columns to pivot
            var_name: Name for variable column
            value_name: Name for value column
        """
        # Create array of structs for value columns
        expr = F.array(*[F.struct(F.lit(c).alias(var_name), 
                                  F.col(c).alias(value_name)) 
                        for c in value_cols])
        
        df_long = df.select(*id_cols, F.explode(expr).alias('_tmp')) \
                    .select(*id_cols, 
                           F.col(f'_tmp.{var_name}').alias(var_name),
                           F.col(f'_tmp.{value_name}').alias(value_name))
        
        return df_long
    
    @staticmethod
    def safe_divide(numerator: str, denominator: str, default_value: float = 0.0) -> F.Column:
        """
        Safe division handling null and zero denominators.
        
        Args:
            numerator: Numerator column name
            denominator: Denominator column name
            default_value: Default value for division by zero
        """
        return F.when(
            (F.col(denominator).isNull()) | (F.col(denominator) == 0),
            F.lit(default_value)
        ).otherwise(F.col(numerator) / F.col(denominator))
    
    @staticmethod
    def calculate_percentile(df: DataFrame, column: str, percentiles: List[float]) -> Dict[float, float]:
        """
        Calculate percentiles for a column.
        
        Args:
            df: Input DataFrame
            column: Column name
            percentiles: List of percentiles (0-1)
        """
        return dict(zip(percentiles, df.approxQuantile(column, percentiles, 0.01)))


# ============================================================================
# DATA QUALITY VALIDATION FRAMEWORK
# ============================================================================

@dataclass
class DataQualityResult:
    """Data quality check result."""
    check_name: str
    status: str  # PASS, FAIL, WARNING
    details: Dict[str, Any]
    timestamp: str = None
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()


class DataQualityValidator:
    """
    Reusable data quality validation framework.
    """
    
    def __init__(self, spark: SparkSession, logger: JobLogger = None):
        """
        Initialize data quality validator.
        
        Args:
            spark: SparkSession
            logger: JobLogger instance
        """
        self.spark = spark
        self.logger = logger
        self.results = []
    
    def check_null_percentage(self, df: DataFrame, column: str, 
                             threshold: float = 0.1) -> DataQualityResult:
        """
        Check null percentage in column.
        
        Args:
            df: Input DataFrame
            column: Column name
            threshold: Maximum allowed null percentage
        """
        total_count = df.count()
        null_count = df.filter(F.col(column).isNull()).count()
        null_percentage = null_count / total_count if total_count > 0 else 0
        
        status = 'PASS' if null_percentage <= threshold else 'FAIL'
        
        result = DataQualityResult(
            check_name=f'null_check_{column}',
            status=status,
            details={
                'column': column,
                'null_count': null_count,
                'total_count': total_count,
                'null_percentage': null_percentage,
                'threshold': threshold
            }
        )
        
        self.results.append(result)
        if self.logger:
            self.logger.logger.info(f"Null check for {column}: {status} ({null_percentage:.2%})")
        
        return result
    
    def check_uniqueness(self, df: DataFrame, columns: List[str],
                        threshold: float = 1.0) -> DataQualityResult:
        """
        Check uniqueness of column combination.
        
        Args:
            df: Input DataFrame
            columns: List of columns
            threshold: Minimum required uniqueness ratio
        """
        total_count = df.count()
        distinct_count = df.select(columns).distinct().count()
        uniqueness_ratio = distinct_count / total_count if total_count > 0 else 0
        
        status = 'PASS' if uniqueness_ratio >= threshold else 'FAIL'
        
        result = DataQualityResult(
            check_name=f'uniqueness_check_{"_".join(columns)}',
            status=status,
            details={
                'columns': columns,
                'total_count': total_count,
                'distinct_count': distinct_count,
                'uniqueness_ratio': uniqueness_ratio,
                'threshold': threshold
            }
        )
        
        self.results.append(result)
        if self.logger:
            self.logger.logger.info(
                f"Uniqueness check for {columns}: {status} ({uniqueness_ratio:.2%})"
            )
        
        return result
    
    def check_referential_integrity(self, df_child: DataFrame, df_parent: DataFrame,
                                   child_keys: List[str], parent_keys: List[str]) -> DataQualityResult:
        """
        Check referential integrity between child and parent DataFrames.
        
        Args:
            df_child: Child DataFrame
            df_parent: Parent DataFrame
            child_keys: Foreign key columns in child
            parent_keys: Primary key columns in parent
        """
        # Find orphan records
        join_condition = [df_child[c] == df_parent[p] 
                         for c, p in zip(child_keys, parent_keys)]
        
        orphan_count = df_child.join(
            df_parent.select(parent_keys),
            join_condition,
            'left_anti'
        ).count()
        
        total_count = df_child.count()
        orphan_percentage = orphan_count / total_count if total_count > 0 else 0
        
        status = 'PASS' if orphan_count == 0 else 'FAIL'
        
        result = DataQualityResult(
            check_name=f'referential_integrity_{"_".join(child_keys)}',
            status=status,
            details={
                'child_keys': child_keys,
                'parent_keys': parent_keys,
                'orphan_count': orphan_count,
                'total_count': total_count,
                'orphan_percentage': orphan_percentage
            }
        )
        
        self.results.append(result)
        if self.logger:
            self.logger.logger.info(
                f"Referential integrity check: {status} ({orphan_count} orphans)"
            )
        
        return result
    
    def check_value_range(self, df: DataFrame, column: str,
                         min_value: Any = None, max_value: Any = None) -> DataQualityResult:
        """
        Check if values are within specified range.
        
        Args:
            df: Input DataFrame
            column: Column name
            min_value: Minimum allowed value
            max_value: Maximum allowed value
        """
        condition = F.lit(True)
        
        if min_value is not None:
            condition = condition & (F.col(column) >= min_value)
        
        if max_value is not None:
            condition = condition & (F.col(column) <= max_value)
        
        total_count = df.count()
        valid_count = df.filter(condition).count()
        invalid_count = total_count - valid_count
        
        status = 'PASS' if invalid_count == 0 else 'FAIL'
        
        result = DataQualityResult(
            check_name=f'range_check_{column}',
            status=status,
            details={
                'column': column,
                'min_value': min_value,
                'max_value': max_value,
                'valid_count': valid_count,
                'invalid_count': invalid_count,
                'total_count': total_count
            }
        )
        
        self.results.append(result)
        if self.logger:
            self.logger.logger.info(
                f"Range check for {column}: {status} ({invalid_count} invalid)"
            )
        
        return result
    
    def check_data_freshness(self, df: DataFrame, date_column: str,
                           max_age_days: int = 1) -> DataQualityResult:
        """
        Check data freshness based on date column.
        
        Args:
            df: Input DataFrame
            date_column: Date column name
            max_age_days: Maximum allowed age in days
        """
        max_date = df.agg(F.max(date_column)).collect()[0][0]
        current_date = datetime.now().date()
        
        if max_date:
            age_days = (current_date - max_date).days if hasattr(max_date, '__sub__') else 0
        else:
            age_days = None
        
        status = 'PASS' if age_days is not None and age_days <= max_age_days else 'FAIL'
        
        result = DataQualityResult(
            check_name=f'freshness_check_{date_column}',
            status=status,
            details={
                'date_column': date_column,
                'max_date': str(max_date) if max_date else None,
                'current_date': str(current_date),
                'age_days': age_days,
                'max_age_days': max_age_days
            }
        )
        
        self.results.append(result)
        if self.logger:
            self.logger.logger.info(
                f"Freshness check for {date_column}: {status} (age: {age_days} days)"
            )
        
        return result
    
    def check_schema_compliance(self, df: DataFrame, 
                               expected_schema: StructType) -> DataQualityResult:
        """
        Check if DataFrame schema matches expected schema.
        
        Args:
            df: Input DataFrame
            expected_schema: Expected StructType
        """
        actual_fields = {field.name: field.dataType for field in df.schema.fields}
        expected_fields = {field.name: field.dataType for field in expected_schema.fields}
        
        missing_fields = set(expected_fields.keys()) - set(actual_fields.keys())
        extra_fields = set(actual_fields.keys()) - set(expected_fields.keys())
        
        type_mismatches = {}
        for field_name in set(actual_fields.keys()) & set(expected_fields.keys()):
            if actual_fields[field_name] != expected_fields[field_name]:
                type_mismatches[field_name] = {
                    'actual': str(actual_fields[field_name]),
                    'expected': str(expected_fields[field_name])
                }
        
        status = 'PASS' if not (missing_fields or extra_fields or type_mismatches) else 'FAIL'
        
        result = DataQualityResult(
            check_name='schema_compliance_check',
            status=status,
            details={
                'missing_fields': list(missing_fields),
                'extra_fields': list(extra_fields),
                'type_mismatches': type_mismatches
            }
        )
        
        self.results.append(result)
        if self.logger:
            self.logger.logger.info(f"Schema compliance check: {status}")
        
        return result
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all validation results."""
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r.status == 'PASS')
        failed_checks = sum(1 for r in self.results if r.status == 'FAIL')
        
        return {
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': failed_checks,
            'pass_rate': passed_checks / total_checks if total_checks > 0 else 0,
            'results': [asdict(r) for r in self.results]
        }


# ============================================================================
# DATA LINEAGE TRACKING
# ============================================================================

@dataclass
class LineageNode:
    """Represents a node in data lineage."""
    node_id: str
    node_type: str  # SOURCE, TRANSFORMATION, TARGET
    name: str
    details: Dict[str, Any]
    timestamp: str = None
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()


class LineageTracker:
    """
    Tracks data lineage throughout transformations.
    """
    
    def __init__(self, job_name: str):
        """
        Initialize lineage tracker.
        
        Args:
            job_name: Name of the job
        """
        self.job_name = job_name
        self.nodes = []
        self.edges = []
    
    def add_source(self, source_id: str, source_name: str, 
                   details: Dict[str, Any] = None) -> str:
        """
        Add a source node.
        
        Args:
            source_id: Unique source identifier
            source_name: Source name
            details: Additional details
        """
        node = LineageNode(
            node_id=source_id,
            node_type='SOURCE',
            name=source_name,
            details=details or {}
        )
        self.nodes.append(node)
        return source_id
    
    def add_transformation(self, transform_id: str, transform_name: str,
                          source_ids: List[str], details: Dict[str, Any] = None) -> str:
        """
        Add a transformation node.
        
        Args:
            transform_i