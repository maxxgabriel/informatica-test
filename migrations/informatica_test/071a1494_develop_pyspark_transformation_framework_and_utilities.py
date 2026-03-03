import logging
import json
import time
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from functools import wraps
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import hashlib
import yaml
import os
from pathlib import Path


class ConfigurationManager:
    """
    Manages configuration for different environments and provides centralized config access
    """
    
    def __init__(self, config_path: str, environment: str = "dev"):
        """
        Initialize configuration manager
        
        Args:
            config_path: Path to configuration file (YAML or JSON)
            environment: Environment name (dev, qa, prod)
        """
        self.config_path = config_path
        self.environment = environment
        self.config = self._load_config()
        
    def _load_config(self) -> Dict:
        """Load configuration from file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
            
        with open(self.config_path, 'r') as f:
            if self.config_path.endswith('.yaml') or self.config_path.endswith('.yml'):
                full_config = yaml.safe_load(f)
            elif self.config_path.endswith('.json'):
                full_config = json.load(f)
            else:
                raise ValueError("Configuration file must be YAML or JSON")
                
        return full_config.get(self.environment, full_config)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key (supports nested keys with dot notation)
        
        Args:
            key: Configuration key (e.g., 'database.host')
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
    
    def get_connection_config(self, connection_name: str) -> Dict:
        """Get connection configuration by name"""
        return self.config.get('connections', {}).get(connection_name, {})
    
    def get_all(self) -> Dict:
        """Get all configuration"""
        return self.config


class ConnectionManager:
    """
    Manages database connections with connection pooling and retry logic
    """
    
    def __init__(self, config_manager: ConfigurationManager):
        """
        Initialize connection manager
        
        Args:
            config_manager: ConfigurationManager instance
        """
        self.config_manager = config_manager
        self._connections = {}
        
    def get_jdbc_properties(self, connection_name: str) -> Dict:
        """
        Get JDBC connection properties
        
        Args:
            connection_name: Name of connection in configuration
            
        Returns:
            Dictionary of JDBC properties
        """
        conn_config = self.config_manager.get_connection_config(connection_name)
        
        properties = {
            "user": conn_config.get('username'),
            "password": conn_config.get('password'),
            "driver": conn_config.get('driver'),
        }
        
        if 'options' in conn_config:
            properties.update(conn_config['options'])
            
        return properties
    
    def get_jdbc_url(self, connection_name: str) -> str:
        """Get JDBC URL for connection"""
        conn_config = self.config_manager.get_connection_config(connection_name)
        return conn_config.get('url')
    
    def read_jdbc(self, spark: SparkSession, connection_name: str, 
                  table: str = None, query: str = None, 
                  partitioning_config: Dict = None) -> DataFrame:
        """
        Read data from JDBC source with connection pooling
        
        Args:
            spark: SparkSession
            connection_name: Name of connection
            table: Table name to read
            query: SQL query to execute (alternative to table)
            partitioning_config: Dict with numPartitions, partitionColumn, lowerBound, upperBound
            
        Returns:
            DataFrame with data
        """
        if not table and not query:
            raise ValueError("Either table or query must be provided")
            
        jdbc_url = self.get_jdbc_url(connection_name)
        properties = self.get_jdbc_properties(connection_name)
        
        reader = spark.read.format("jdbc").option("url", jdbc_url)
        
        for key, value in properties.items():
            reader = reader.option(key, value)
            
        if partitioning_config:
            for key, value in partitioning_config.items():
                reader = reader.option(key, value)
                
        if query:
            reader = reader.option("query", query)
        else:
            reader = reader.option("dbtable", table)
            
        return reader.load()
    
    def write_jdbc(self, df: DataFrame, connection_name: str, 
                   table: str, mode: str = "append",
                   batch_size: int = 10000) -> None:
        """
        Write data to JDBC target
        
        Args:
            df: DataFrame to write
            connection_name: Name of connection
            table: Target table name
            mode: Write mode (append, overwrite, ignore, error)
            batch_size: Batch size for inserts
        """
        jdbc_url = self.get_jdbc_url(connection_name)
        properties = self.get_jdbc_properties(connection_name)
        properties["batchsize"] = batch_size
        
        df.write.jdbc(url=jdbc_url, table=table, mode=mode, properties=properties)


class LoggingFramework:
    """
    Centralized logging framework with structured logging and metric collection
    """
    
    def __init__(self, job_name: str, log_level: str = "INFO", 
                 log_file: str = None):
        """
        Initialize logging framework
        
        Args:
            job_name: Name of the job
            log_level: Logging level
            log_file: Path to log file (optional)
        """
        self.job_name = job_name
        self.job_run_id = f"{job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.start_time = time.time()
        self.metrics = {
            'job_name': job_name,
            'job_run_id': self.job_run_id,
            'start_time': datetime.now().isoformat(),
            'steps': []
        }
        
        self.logger = logging.getLogger(job_name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(job_run_id)s] - %(message)s'
        )
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
        self.logger = logging.LoggerAdapter(self.logger, {'job_run_id': self.job_run_id})
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self.logger.info(message, extra=kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        self.logger.error(message, extra=kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self.logger.warning(message, extra=kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self.logger.debug(message, extra=kwargs)
    
    def log_step_start(self, step_name: str) -> Dict:
        """
        Log start of processing step
        
        Args:
            step_name: Name of the step
            
        Returns:
            Step context dictionary
        """
        step_context = {
            'step_name': step_name,
            'start_time': time.time(),
            'status': 'RUNNING'
        }
        self.info(f"Starting step: {step_name}")
        return step_context
    
    def log_step_end(self, step_context: Dict, record_count: int = None, 
                     status: str = "SUCCESS", error_message: str = None):
        """
        Log end of processing step
        
        Args:
            step_context: Step context from log_step_start
            record_count: Number of records processed
            status: Step status (SUCCESS, FAILED)
            error_message: Error message if failed
        """
        duration = time.time() - step_context['start_time']
        step_context['duration_seconds'] = duration
        step_context['status'] = status
        step_context['record_count'] = record_count
        step_context['error_message'] = error_message
        
        self.metrics['steps'].append(step_context)
        
        log_message = f"Completed step: {step_context['step_name']} - " \
                     f"Duration: {duration:.2f}s - Status: {status}"
        
        if record_count is not None:
            log_message += f" - Records: {record_count}"
            
        if status == "SUCCESS":
            self.info(log_message)
        else:
            self.error(log_message + f" - Error: {error_message}")
    
    def log_dataframe_stats(self, df: DataFrame, df_name: str):
        """
        Log DataFrame statistics
        
        Args:
            df: DataFrame to analyze
            df_name: Name of the DataFrame
        """
        count = df.count()
        columns = len(df.columns)
        self.info(f"DataFrame '{df_name}' - Rows: {count}, Columns: {columns}")
        
    def get_job_metrics(self) -> Dict:
        """Get all job metrics"""
        self.metrics['end_time'] = datetime.now().isoformat()
        self.metrics['total_duration_seconds'] = time.time() - self.start_time
        return self.metrics
    
    def log_job_summary(self):
        """Log job summary with all metrics"""
        metrics = self.get_job_metrics()
        self.info(f"Job Summary: {json.dumps(metrics, indent=2)}")


class DataQualityValidator:
    """
    Reusable data quality validation functions
    """
    
    def __init__(self, logger: LoggingFramework = None):
        """
        Initialize data quality validator
        
        Args:
            logger: LoggingFramework instance
        """
        self.logger = logger
        self.validation_results = []
    
    def validate_not_null(self, df: DataFrame, columns: List[str], 
                          threshold: float = 0.0) -> Dict:
        """
        Validate columns are not null
        
        Args:
            df: DataFrame to validate
            columns: List of column names
            threshold: Maximum allowed null percentage (0.0 to 1.0)
            
        Returns:
            Validation result dictionary
        """
        total_count = df.count()
        results = {
            'validation_type': 'not_null',
            'total_records': total_count,
            'columns': {}
        }
        
        for col in columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_pct = null_count / total_count if total_count > 0 else 0
            
            passed = null_pct <= threshold
            
            results['columns'][col] = {
                'null_count': null_count,
                'null_percentage': null_pct,
                'threshold': threshold,
                'passed': passed
            }
            
            if self.logger:
                status = "PASSED" if passed else "FAILED"
                self.logger.info(
                    f"NOT_NULL validation for '{col}': {status} - "
                    f"Nulls: {null_count} ({null_pct:.2%})"
                )
        
        self.validation_results.append(results)
        return results
    
    def validate_unique(self, df: DataFrame, columns: List[str], 
                       threshold: float = 0.0) -> Dict:
        """
        Validate columns have unique values
        
        Args:
            df: DataFrame to validate
            columns: List of column names
            threshold: Maximum allowed duplicate percentage
            
        Returns:
            Validation result dictionary
        """
        total_count = df.count()
        results = {
            'validation_type': 'unique',
            'total_records': total_count,
            'columns': {}
        }
        
        for col in columns:
            distinct_count = df.select(col).distinct().count()
            duplicate_count = total_count - distinct_count
            duplicate_pct = duplicate_count / total_count if total_count > 0 else 0
            
            passed = duplicate_pct <= threshold
            
            results['columns'][col] = {
                'distinct_count': distinct_count,
                'duplicate_count': duplicate_count,
                'duplicate_percentage': duplicate_pct,
                'threshold': threshold,
                'passed': passed
            }
            
            if self.logger:
                status = "PASSED" if passed else "FAILED"
                self.logger.info(
                    f"UNIQUE validation for '{col}': {status} - "
                    f"Duplicates: {duplicate_count} ({duplicate_pct:.2%})"
                )
        
        self.validation_results.append(results)
        return results
    
    def validate_range(self, df: DataFrame, column: str, 
                      min_value: Any = None, max_value: Any = None,
                      threshold: float = 0.0) -> Dict:
        """
        Validate numeric column values are within range
        
        Args:
            df: DataFrame to validate
            column: Column name
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            threshold: Maximum allowed out-of-range percentage
            
        Returns:
            Validation result dictionary
        """
        total_count = df.count()
        
        condition = F.lit(True)
        if min_value is not None:
            condition = condition & (F.col(column) >= min_value)
        if max_value is not None:
            condition = condition & (F.col(column) <= max_value)
            
        valid_count = df.filter(condition).count()
        invalid_count = total_count - valid_count
        invalid_pct = invalid_count / total_count if total_count > 0 else 0
        
        passed = invalid_pct <= threshold
        
        results = {
            'validation_type': 'range',
            'column': column,
            'total_records': total_count,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'invalid_percentage': invalid_pct,
            'min_value': min_value,
            'max_value': max_value,
            'threshold': threshold,
            'passed': passed
        }
        
        if self.logger:
            status = "PASSED" if passed else "FAILED"
            self.logger.info(
                f"RANGE validation for '{column}': {status} - "
                f"Out of range: {invalid_count} ({invalid_pct:.2%})"
            )
        
        self.validation_results.append(results)
        return results
    
    def validate_format(self, df: DataFrame, column: str, 
                       pattern: str, threshold: float = 0.0) -> Dict:
        """
        Validate column values match regex pattern
        
        Args:
            df: DataFrame to validate
            column: Column name
            pattern: Regular expression pattern
            threshold: Maximum allowed invalid percentage
            
        Returns:
            Validation result dictionary
        """
        total_count = df.count()
        valid_count = df.filter(F.col(column).rlike(pattern)).count()
        invalid_count = total_count - valid_count
        invalid_pct = invalid_count / total_count if total_count > 0 else 0
        
        passed = invalid_pct <= threshold
        
        results = {
            'validation_type': 'format',
            'column': column,
            'total_records': total_count,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'invalid_percentage': invalid_pct,
            'pattern': pattern,
            'threshold': threshold,
            'passed': passed
        }
        
        if self.logger:
            status = "PASSED" if passed else "FAILED"
            self.logger.info(
                f"FORMAT validation for '{column}': {status} - "
                f"Invalid: {invalid_count} ({invalid_pct:.2%})"
            )
        
        self.validation_results.append(results)
        return results
    
    def validate_referential_integrity(self, df_child: DataFrame, 
                                      df_parent: DataFrame,
                                      child_key: str, parent_key: str,
                                      threshold: float = 0.0) -> Dict:
        """
        Validate referential integrity between two DataFrames
        
        Args:
            df_child: Child DataFrame
            df_parent: Parent DataFrame
            child_key: Foreign key column in child
            parent_key: Primary key column in parent
            threshold: Maximum allowed orphan percentage
            
        Returns:
            Validation result dictionary
        """
        total_count = df_child.count()
        
        orphans = df_child.join(
            df_parent.select(parent_key),
            df_child[child_key] == df_parent[parent_key],
            "left_anti"
        ).count()
        
        orphan_pct = orphans / total_count if total_count > 0 else 0
        passed = orphan_pct <= threshold
        
        results = {
            'validation_type': 'referential_integrity',
            'total_records': total_count,
            'orphan_count': orphans,
            'orphan_percentage': orphan_pct,
            'child_key': child_key,
            'parent_key': parent_key,
            'threshold': threshold,
            'passed': passed
        }
        
        if self.logger:
            status = "PASSED" if passed else "FAILED"
            self.logger.info(
                f"REFERENTIAL INTEGRITY validation: {status} - "
                f"Orphans: {orphans} ({orphan_pct:.2%})"
            )
        
        self.validation_results.append(results)
        return results
    
    def get_all_results(self) -> List[Dict]:
        """Get all validation results"""
        return self.validation_results
    
    def all_passed(self) -> bool:
        """Check if all validations passed"""
        return all(
            all(col_result.get('passed', True) 
                for col_result in result.get('columns', {}).values())
            if 'columns' in result
            else result.get('passed', True)
            for result in self.validation_results
        )


class TransformationUtilities:
    """
    Common transformation utilities for PySpark
    """
    
    @staticmethod
    def standardize_column_names(df: DataFrame, 
                                 case: str = "lower",
                                 replace_spaces: bool = True,
                                 space_char: str = "_") -> DataFrame:
        """
        Standardize column names
        
        Args:
            df: Input DataFrame
            case: 'lower', 'upper', or 'title'
            replace_spaces: Replace spaces in column names
            space_char: Character to replace spaces with
            
        Returns:
            DataFrame with standardized column names
        """
        for col in df.columns:
            new_col = col
            
            if replace_spaces:
                new_col = new_col.replace(" ", space_char)
                
            if case == "lower":
                new_col = new_col.lower()
            elif case == "upper":
                new_col = new_col.upper()
            elif case == "title":
                new_col = new_col.title()
                
            if new_col != col:
                df = df.withColumnRenamed(col, new_col)
                
        return df
    
    @staticmethod
    def add_audit_columns(df: DataFrame, 
                         job_run_id: str = None,
                         add_hash: bool = False,
                         hash_columns: List[str] = None) -> DataFrame:
        """
        Add standard audit columns
        
        Args:
            df: Input DataFrame
            job_run_id: Job run identifier
            add_hash: Add row hash column
            hash_columns: Columns to include in hash (all if None)
            
        Returns:
            DataFrame with audit columns
        """
        df = df.withColumn("insert_timestamp", F.current_timestamp())
        df = df.withColumn("update_timestamp", F.current_timestamp())
        
        if job_run_id:
            df = df.withColumn("job_run_id", F.lit(job_run_id))
            
        if add_hash:
            cols_to_hash = hash_columns if hash_columns else df.columns
            df = df.withColumn(
                "row_hash",
                F.md5(F.concat_ws("||", *[F.col(c) for c in cols_to_hash]))
            )
            
        return df
    
    @staticmethod
    def remove_duplicates(df: DataFrame, 
                         key_columns: List[str],
                         order_column: str = None,
                         keep: str = "last") -> DataFrame:
        """
        Remove duplicate rows based on key columns
        
        Args:
            df: Input DataFrame
            key_columns: Columns to identify duplicates
            order_column: Column to order by when selecting which duplicate to keep
            keep: 'first' or 'last' (requires order_column)
            
        Returns:
            DataFrame with duplicates removed
        """
        if order_column:
            window_spec = Window.partitionBy(*key_columns).orderBy(
                F.col(order_column).desc() if keep == "last" 
                else F.col(order_column).asc()
            )
            df = df.withColumn("row_num", F.row_number().over(window_spec))
            df = df.filter(F.col("row_num") == 1).drop("row_num")
        else:
            df = df.dropDuplicates(key_columns)
            
        return df
    
    @staticmethod
    def apply_scd_type2(df_new: DataFrame, 
                       df_existing: DataFrame,
                       key_columns: List[str],
                       compare_columns: List[str] = None,
                       effective_date_col: str = "effective_date",
                       end_date_col: str = "end_date",
                       current_flag_col: str = "is_current") -> DataFrame:
        """
        Apply SCD Type 2 logic
        
        Args:
            df_new: New/incoming data
            df_existing: Existing historical data
            key_columns: Business key columns
            compare_columns: Columns to compare for changes (all if None)
            effective_date_col: Effective date column name
            end_date_col: End date column name
            current_flag_col: Current flag column name
            
        Returns:
            DataFrame with SCD Type 2 applied
        """
        if compare_columns is None:
            compare_columns = [c for c in df_new.columns if c not in key_columns]
        
        current_date = F.current_date()
        max_date = F.lit("9999-12-31").cast("date")
        
        df_new = df_new.withColumn(effective_date_col, current_date)
        df_new = df_new.withColumn(end_date_col, max_date)
        df_new = df_new.withColumn(current_flag_col, F.lit(True))
        
        df_current = df_existing.filter(F.col(current_flag_col) == True)
        
        join_condition = [df_new[k] == df_current[k] for k in key_columns]
        df_joined = df_new.alias("new").join(
            df_current.alias("current"),
            join_condition,
            "left"
        )
        
        change_condition = F.lit(False)
        for col in compare_columns:
            change_condition = change_condition | (
                F.col(f"new.{col}") != F.col(f"current.{col}")
            )
        
        df_changed = df_joined.filter(
            F.col(f"current.{key_columns[0]}").isNotNull() & change_condition
        ).select("new.*")
        
        df_unchanged = df_joined.filter(
            F.col(f"current.{key_columns[0]}").isNotNull() & ~change_condition
        ).select("current.*")
        
        df_new_records = df_joined.filter(
            F.col(f"current.{key_columns[0]}").isNull()
        ).select("new.*")
        
        df_expired = df_existing.join(
            df_changed.select(*key_columns),
            key_columns,
            "inner"
        ).withColumn(end_date_col, F.date_sub(current_date, 1)) \
         .withColumn(current_flag_col, F.lit(False))
        
        df_result = df_expired.union(df_changed).union(df_unchanged).union(df_new_records)
        
        df_historical = df_existing.filter(F.col(current_flag_col) == False)
        
        return df_result.union(df_historical)
    
    @staticmethod
    def pivot_data(df: DataFrame, 
                   group_columns: List[str],
                   pivot_column: str,
                   value_column: str,
                   agg_func: str = "sum") -> DataFrame:
        """
        Pivot DataFrame
        
        Args:
            df: Input DataFrame
            group_columns: Columns to group by
            pivot_column: Column to pivot
            value_column: Column with values to aggregate
            agg_func: Aggregation function (sum, avg, max, min, count)
            
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
        
        agg = agg_functions.get(agg_func, F.sum)
        
        return df.groupBy(*group_columns).pivot(pivot_column).agg(agg(value_column))
    
    @staticmethod
    def unpivot_data(df: DataFrame, 
                    id_columns: List[str],
                    value_columns: List[str],
                    var_name: str = "variable",
                    value_name: str = "value") -> DataFrame:
        """
        Unpivot DataFrame
        
        Args:
            df: Input DataFrame
            id_columns: Columns to keep as identifiers
            value_columns: Columns to unpivot
            var_name: Name for variable column
            value_name: Name for value column
            
        Returns:
            Unpivoted DataFrame
        """
        unpivot_expr = f"stack({len(value_columns)}, "
        unpivot_expr += ", ".join([f"'{col}', `{col}`" for col in value_columns])
        unpivot_expr += f") as ({var_name}, {value_name})"
        
        return df.select(*id_columns, F.expr(unpivot_expr))
    
    @staticmethod
    def apply_business_rules(df: DataFrame, 
                            rules: List[Dict[str, Any]]) -> DataFrame:
        """
        Apply business rules to DataFrame
        
        Args:
            df: Input DataFrame
            rules: List of rule dictionaries with 'condition' and 'value' keys
                   Example: [{'name': 'new_col', 'condition': 'col1 > 100', 'value': 'col2 * 2'}]
            
        Returns:
            DataFrame with business rules applied
        """
        for rule in rules:
            column_name = rule['name']
            condition = rule.get('condition')
            value_expr = rule['value']
            default_value = rule.get('default')
            
            if condition:
                df = df.withColumn(
                    column_name,
                    F.when(F.expr(condition), F.expr(value_expr))
                     .otherwise(F.expr(default_value) if default_value else None)
                )
            else:
                df = df.withColumn(column_name, F.expr(value_expr))
                
        return df
    
    @staticmethod
    def handle_nulls(df: DataFrame, 
                    strategy: str = "drop",
                    columns: List[str] = None,
                    fill_values: Dict[str, Any] = None) -> DataFrame:
        """
        Handle null values in DataFrame
        
        Args:
            df: Input DataFrame
            strategy: 'drop', 'fill', or 'custom'
            columns: Columns to apply strategy to (all if None)
            fill_values: Dictionary of column: fill_value pairs for fill strategy
            
        Returns:
            DataFrame with nulls handled
        """
        if strategy == "drop":
            return df.dropna(subset=columns)
        elif strategy == "fill":
            if fill_values:
                return df.fillna(fill_values)
            else:
                return df.fillna(0)
        else:
            return df


class DataLineageTracker:
    """
    Track data lineage and transformations
    """
    
    def __init__(self, job_name: str):
        """
        Initialize lineage tracker
        
        Args:
            job_name: Name of the job
        """
        self.job_name = job_name
        self.lineage = {
            'job_name': job_name,
            'job_run_id': f"{job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'start_time': datetime.now().isoformat(),
            'sources': [],
            'transformations': [],
            'targets': []
        }
    
    def track_source(self, source_name: str, source_type: str, 
                    location: str, record_count: int = None,
                    metadata: Dict = None):
        """
        Track data source
        
        Args:
            source_name: Name of the source
            source_type: Type of source (table, file, api, etc.)
            location: Source location/path
            record_count: Number of records read
            metadata: Additional metadata
        """
        source_info = {
            'name': source_name,
            'type': source_type,
            'location': location,
            'record_count': record_count,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        self.lineage['sources'].append(source_info)
    
    def track_transformation(self, transformation_name: str, 
                           transformation_type: str,
                           input_datasets: List[str],
                           output_dataset: str,
                           description: str = None,
                           record_count_before: int = None,
                           record_count_after: int = None,
                           columns_affected: List[str] = None):
        """
        Track transformation