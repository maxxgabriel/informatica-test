"""
Configuration Manager
Handles environment-based configuration loading and validation.
"""

import os
import yaml
from pathlib import Path
from typing import Any, Dict, Optional
from dataclasses import dataclass
import re


class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing."""
    pass


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    port: int
    username: str
    password: str
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600
    pool_pre_ping: bool = True
    echo: bool = False
    
    # Oracle specific
    service_name: Optional[str] = None
    
    # SQL Server specific
    database: Optional[str] = None
    driver: Optional[str] = None


class ConfigManager:
    """
    Manages application configuration with environment variable substitution.
    Supports multiple environments (dev, staging, prod).
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to config file. If None, searches default locations.
        """
        self.config_path = config_path or self._find_config_file()
        self._config: Dict[str, Any] = {}
        self._load_config()
    
    def _find_config_file(self) -> str:
        """Find configuration file in standard locations."""
        search_paths = [
            Path("config.yaml"),
            Path("config/config.yaml"),
            Path("/etc/etl/config.yaml"),
            Path.home() / ".etl" / "config.yaml"
        ]
        
        for path in search_paths:
            if path.exists():
                return str(path)
        
        raise ConfigurationError(
            f"Configuration file not found in any of: {search_paths}"
        )
    
    def _load_config(self) -> None:
        """Load and parse configuration file."""
        try:
            with open(self.config_path, 'r') as f:
                raw_config = yaml.safe_load(f)
            
            self._config = self._substitute_env_vars(raw_config)
            self._validate_config()
            
        except FileNotFoundError:
            raise ConfigurationError(
                f"Configuration file not found: {self.config_path}"
            )
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in config file: {e}")
    
    def _substitute_env_vars(self, config: Any) -> Any:
        """
        Recursively substitute environment variables in configuration.
        
        Format: ${VAR_NAME:default_value}
        """
        if isinstance(config, dict):
            return {
                key: self._substitute_env_vars(value)
                for key, value in config.items()
            }
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str):
            return self._substitute_string(config)
        else:
            return config
    
    def _substitute_string(self, value: str) -> Any:
        """Substitute environment variables in a string."""
        pattern = r'\$\{([^:}]+)(?::([^}]*))?\}'
        
        def replacer(match):
            var_name = match.group(1)
            default_value = match.group(2) if match.group(2) is not None else ""
            env_value = os.getenv(var_name, default_value)
            return env_value
        
        result = re.sub(pattern, replacer, value)
        
        # Try to convert to appropriate type
        if result.lower() == 'true':
            return True
        elif result.lower() == 'false':
            return False
        elif result.isdigit():
            return int(result)
        try:
            return float(result)
        except ValueError:
            return result
    
    def _validate_config(self) -> None:
        """Validate required configuration sections exist."""
        required_sections = ['application', 'logging', 'databases']
        
        for section in required_sections:
            if section not in self._config:
                raise ConfigurationError(
                    f"Required configuration section missing: {section}"
                )
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-notation path.
        
        Args:
            key_path: Dot-separated path (e.g., 'databases.oracle.source.host')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key_path.split('.')
        value = self._config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def get_database_config(
        self,
        db_type: str,
        connection_name: str
    ) -> DatabaseConfig:
        """
        Get database configuration.
        
        Args:
            db_type: 'oracle' or 'sqlserver'
            connection_name: 'source' or 'target'
            
        Returns:
            DatabaseConfig object
        """
        base_path = f"databases.{db_type}.{connection_name}"
        config = self.get(base_path)
        
        if not config:
            raise ConfigurationError(
                f"Database configuration not found: {base_path}"
            )
        
        pool_config = config.get('pool', {})
        
        common_params = {
            'host': config['host'],
            'port': config['port'],
            'username': config['username'],
            'password': config['password'],
            'pool_size': pool_config.get('pool_size', 5),
            'max_overflow': pool_config.get('max_overflow', 10),
            'pool_timeout': pool_config.get('pool_timeout', 30),
            'pool_recycle': pool_config.get('pool_recycle', 3600),
            'pool_pre_ping': pool_config.get('pool_pre_ping', True),
            'echo': pool_config.get('echo', False),
        }
        
        if db_type == 'oracle':
            common_params['service_name'] = config['service_name']
        elif db_type == 'sqlserver':
            common_params['database'] = config['database']
            common_params['driver'] = config.get('driver')
        
        return DatabaseConfig(**common_params)
    
    def get_environment(self) -> str:
        """Get current environment (dev, staging, prod)."""
        return self.get('environment', 'development')
    
    def get_log_level(self) -> str:
        """Get logging level."""
        return self.get('logging.level', 'INFO')
    
    def get_log_format(self) -> str:
        """Get logging format (json or text)."""
        return self.get('logging.format', 'json')
    
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.get_environment().lower() == 'production'
    
    def __repr__(self) -> str:
        return f"ConfigManager(config_path='{self.config_path}')"


# Singleton instance
_config_instance: Optional[ConfigManager] = None


def get_config(config_path: Optional[str] = None) -> ConfigManager:
    """
    Get singleton configuration manager instance.
    
    Args:
        config_path: Path to config file (only used on first call)
        
    Returns:
        ConfigManager instance
    """
    global _config_instance
    
    if _config_instance is None:
        _config_instance = ConfigManager(config_path)
    
    return _config_instance


def reload_config(config_path: Optional[str] = None) -> ConfigManager:
    """
    Force reload configuration (useful for testing).
    
    Args:
        config_path: Path to config file
        
    Returns:
        New ConfigManager instance
    """
    global _config_instance
    _config_instance = ConfigManager(config_path)
    return _config_instance