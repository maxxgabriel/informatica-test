"""
Structured Logging with Correlation IDs
Provides consistent logging across all ETL components.
"""

import logging
import json
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, Optional
from contextvars import ContextVar
from pathlib import Path
import uuid


# Context variable for correlation ID (thread-safe)
correlation_id_var: ContextVar[Optional[str]] = ContextVar(
    'correlation_id',
    default=None
)


class CorrelationIdFilter(logging.Filter):
    """Add correlation ID to log records."""
    
    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id_var.get() or 'N/A'
        return True


class JsonFormatter(logging.Formatter):
    """Format log records as JSON."""
    
    def __init__(self, include_correlation_id: bool = True):
        super().__init__()
        self.include_correlation_id = include_correlation_id
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }
        
        if self.include_correlation_id:
            log_data['correlation_id'] = getattr(
                record,
                'correlation_id',
                'N/A'
            )
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': ''.join(
                    traceback.format_exception(*record.exc_info)
                )
            }
        
        # Add extra fields
        if hasattr(record, 'extra_fields'):
            log_data.update(record.extra_fields)
        
        return json.dumps(log_data)


class TextFormatter(logging.Formatter):
    """Format log records as human-readable text."""
    
    def __init__(self, include_correlation_id: bool = True):
        if include_correlation_id:
            fmt = (
                '%(asctime)s - %(correlation_id)s - %(name)s - '
                '%(levelname)s - %(message)s'
            )
        else:
            fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        super().__init__(fmt=fmt, datefmt='%Y-%m-%d %H:%M:%S')


class ETLLogger:
    """
    Enhanced logger with correlation ID support and structured logging.
    """
    
    def __init__(
        self,
        name: str,
        level: str = 'INFO',
        format_type: str = 'json',
        output: str = 'stdout',
        log_file: Optional[str] = None,
        include_correlation_id: bool = True,
        mask_sensitive: bool = True
    ):
        """
        Initialize ETL logger.
        
        Args:
            name: Logger name
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format_type: 'json' or 'text'
            output: 'stdout', 'file', or 'both'
            log_file: Path to log file (if output includes 'file')
            include_correlation_id: Include correlation ID in logs
            mask_sensitive: Mask sensitive data in logs
        """
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        self.logger.handlers.clear()
        self.mask_sensitive = mask_sensitive
        
        # Add correlation ID filter
        if include_correlation_id:
            self.logger.addFilter(CorrelationIdFilter())
        
        # Select formatter
        if format_type.lower() == 'json':
            formatter = JsonFormatter(include_correlation_id)
        else:
            formatter = TextFormatter(include_correlation_id)
        
        # Add handlers
        if output in ['stdout', 'both']:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        if output in ['file', 'both']:
            if not log_file:
                raise ValueError("log_file must be specified when output includes 'file'")
            
            # Create log directory if it doesn't exist
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def _mask_sensitive_data(self, data: Any) -> Any:
        """Mask sensitive data in log messages."""
        if not self.mask_sensitive:
            return data
        
        if isinstance(data, dict):
            masked = {}
            sensitive_keys = {
                'password', 'passwd', 'secret', 'token', 
                'api_key', 'credential', 'authorization'
            }
            
            for key, value in data.items():
                if any(s in key.lower() for s in sensitive_keys):
                    masked[key] = '***MASKED***'
                elif isinstance(value, (dict, list)):
                    masked[key] = self._mask_sensitive_data(value)
                else:
                    masked[key] = value
            return masked
        
        elif isinstance(data, list):
            return [self._mask_sensitive_data(item) for item in data]
        
        elif isinstance(data, str):
            # Mask password-like patterns in strings
            import re
            patterns = [
                (r'password["\']?\s*[:=]\s*["\']?([^"\'&\s]+)', r'password=***MASKED***'),
                (r'token["\']?\s*[:=]\s*["\']?([^"\'&\s]+)', r'token=***MASKED***'),
            ]
            result = data
            for pattern, replacement in patterns:
                result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
            return result
        
        return data
    
    def _log_with_extra(
        self,
        level: int,
        message: str,
        extra_fields: Optional[Dict[str, Any]] = None,
        exc_info: bool = False
    ):
        """Internal method to log with extra fields."""
        if extra_fields:
            masked_extra = self._mask_sensitive_data(extra_fields)
            self.logger.log(
                level,
                message,
                extra={'extra_fields': masked_extra},
                exc_info=exc_info
            )
        else:
            self.logger.log(level, message, exc_info=exc_info)
    
    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self._log_with_extra(logging.DEBUG, message, kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message."""
        self._log_with_extra(logging.INFO, message, kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self._log_with_extra(logging.WARNING, message, kwargs)
    
    def error(self, message: str, exc_info: bool = False, **kwargs):
        """Log error message."""
        self._log_with_extra(logging.ERROR, message, kwargs, exc_info)
    
    def critical(self, message: str, exc_info: bool = False, **kwargs):
        """Log critical message."""
        self._log_with_extra(logging.CRITICAL, message, kwargs, exc_info)
    
    def exception(self, message: str, **kwargs):
        """Log exception with traceback."""
        self._log_with_extra(logging.ERROR, message, kwargs, exc_info=True)


class CorrelationContext:
    """Context manager for correlation ID."""
    
    def __init__(self, correlation_id: Optional[str] = None):
        """
        Initialize correlation context.
        
        Args:
            correlation_id: Correlation ID (generates UUID if None)
        """
        self.correlation_id = correlation_id or str(uuid.uuid4())
        self.token = None
    
    def __enter__(self) -> str:
        """Set correlation ID in context."""
        self.token = correlation_id_var.set(self.correlation_id)
        return self.correlation_id
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Reset correlation ID context."""
        if self.token:
            correlation_id_var.reset(self.token)


def get_correlation_id() -> Optional[str]:
    """Get current correlation ID from context."""
    return correlation_id_var.get()


def set_correlation_id(correlation_id: str):
    """Set correlation ID in context."""
    correlation_id_var.set(correlation_id)


def get_logger(
    name: str,
    config_manager: Optional[Any] = None
) -> ETLLogger:
    """
    Get logger instance with configuration.
    
    Args:
        name: Logger name
        config_manager: ConfigManager instance (optional)
        
    Returns:
        ETLLogger instance
    """
    if config_manager:
        return ETLLogger(
            name=name,
            level=config_manager.get('logging.level', 'INFO'),
            format_type=config_manager.get('logging.format', 'json'),
            output=config_manager.get('logging.output', 'stdout'),
            log_file=config_manager.get('logging.file_path'),
            include_correlation_id=config_manager.get(
                'logging.include_correlation_id',
                True
            ),
            mask_sensitive=config_manager.get(
                'security.mask_sensitive_logs',
                True
            )
        )
    else:
        # Default logger
        return ETLLogger(name=name)