# infrastructure_setup.py
"""
Infrastructure Setup for PySpark Migration from Informatica
Provisions development, testing, and production environments
"""

import os
import json
import yaml
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime


# ============================================================================
# CONFIGURATION CLASSES
# ============================================================================

@dataclass
class SparkConfig:
    """Spark cluster configuration"""
    spark_version: str = "3.5.0"
    scala_version: str = "2.12"
    driver_memory: str = "4g"
    driver_cores: int = 2
    executor_memory: str = "8g"
    executor_cores: int = 4
    num_executors: int = 10
    dynamic_allocation: bool = True
    adaptive_query_execution: bool = True
    
    def to_spark_conf(self) -> Dict[str, str]:
        """Convert to Spark configuration dictionary"""
        return {
            "spark.driver.memory": self.driver_memory,
            "spark.driver.cores": str(self.driver_cores),
            "spark.executor.memory": self.executor_memory,
            "spark.executor.cores": str(self.executor_cores),
            "spark.executor.instances": str(self.num_executors),
            "spark.dynamicAllocation.enabled": str(self.dynamic_allocation).lower(),
            "spark.sql.adaptive.enabled": str(self.adaptive_query_execution).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.sql.hive.convertMetastoreParquet": "true",
            "spark.sql.parquet.mergeSchema": "false",
            "spark.sql.parquet.filterPushdown": "true",
            "spark.sql.orc.filterPushdown": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryoserializer.buffer.max": "512m",
            "spark.network.timeout": "800s",
            "spark.executor.heartbeatInterval": "60s"
        }


@dataclass
class EnvironmentConfig:
    """Environment-specific configuration"""
    name: str
    env_type: str  # dev, test, prod
    region: str
    vpc_cidr: str
    availability_zones: List[str]
    enable_high_availability: bool
    backup_retention_days: int
    
    def __post_init__(self):
        """Validate environment configuration"""
        valid_types = ["dev", "test", "prod"]
        if self.env_type not in valid_types:
            raise ValueError(f"env_type must be one of {valid_types}")


@dataclass
class DatabaseConfig:
    """Database configuration for testing and metadata"""
    db_type: str  # postgres, mysql, oracle
    host: str
    port: int
    database: str
    username: str
    password_secret_name: str
    max_connections: int = 50
    connection_timeout: int = 30
    
    def get_jdbc_url(self) -> str:
        """Generate JDBC connection URL"""
        if self.db_type == "postgres":
            return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        elif self.db_type == "mysql":
            return f"jdbc:mysql://{self.host}:{self.port}/{self.database}"
        elif self.db_type == "oracle":
            return f"jdbc:oracle:thin:@{self.host}:{self.port}:{self.database}"
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")


# ============================================================================
# AWS INFRASTRUCTURE SETUP
# ============================================================================

class AWSInfrastructureProvisioner:
    """Provision AWS infrastructure for PySpark migration"""
    
    def __init__(self, env_config: EnvironmentConfig):
        self.env = env_config
        self.resource_tags = {
            "Project": "Informatica-PySpark-Migration",
            "Environment": env_config.env_type,
            "ManagedBy": "Terraform",
            "CreatedDate": datetime.utcnow().isoformat()
        }
    
    def generate_terraform_vpc(self) -> Dict:
        """Generate Terraform configuration for VPC"""
        return {
            "resource": {
                "aws_vpc": {
                    f"{self.env.name}_vpc": {
                        "cidr_block": self.env.vpc_cidr,
                        "enable_dns_hostnames": True,
                        "enable_dns_support": True,
                        "tags": {
                            **self.resource_tags,
                            "Name": f"{self.env.name}-vpc"
                        }
                    }
                },
                "aws_subnet": {
                    f"{self.env.name}_private_subnet_{i}": {
                        "vpc_id": f"${{aws_vpc.{self.env.name}_vpc.id}}",
                        "cidr_block": self._calculate_subnet_cidr(i),
                        "availability_zone": az,
                        "tags": {
                            **self.resource_tags,
                            "Name": f"{self.env.name}-private-subnet-{i}",
                            "Type": "Private"
                        }
                    }
                    for i, az in enumerate(self.env.availability_zones)
                }
            }
        }
    
    def generate_terraform_emr_cluster(self, spark_config: SparkConfig) -> Dict:
        """Generate Terraform configuration for EMR cluster"""
        return {
            "resource": {
                "aws_emr_cluster": {
                    f"{self.env.name}_spark_cluster": {
                        "name": f"{self.env.name}-spark-cluster",
                        "release_label": f"emr-6.15.0",
                        "applications": ["Spark", "Hive", "Livy", "JupyterHub"],
                        "ec2_attributes": {
                            "subnet_id": f"${{aws_subnet.{self.env.name}_private_subnet_0.id}}",
                            "emr_managed_master_security_group": f"${{aws_security_group.{self.env.name}_emr_master_sg.id}}",
                            "emr_managed_slave_security_group": f"${{aws_security_group.{self.env.name}_emr_slave_sg.id}}",
                            "instance_profile": f"${{aws_iam_instance_profile.{self.env.name}_emr_profile.arn}}"
                        },
                        "master_instance_group": {
                            "instance_type": "m5.2xlarge",
                            "instance_count": 1
                        },
                        "core_instance_group": {
                            "instance_type": "m5.4xlarge",
                            "instance_count": spark_config.num_executors if self.env.env_type == "prod" else 2,
                            "ebs_config": {
                                "size": 500,
                                "type": "gp3",
                                "volumes_per_instance": 2
                            }
                        },
                        "configurations_json": json.dumps([
                            {
                                "Classification": "spark-defaults",
                                "Properties": spark_config.to_spark_conf()
                            },
                            {
                                "Classification": "spark-hive-site",
                                "Properties": {
                                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                                }
                            }
                        ]),
                        "service_role": f"${{aws_iam_role.{self.env.name}_emr_service_role.arn}}",
                        "autoscaling_role": f"${{aws_iam_role.{self.env.name}_emr_autoscaling_role.arn}}",
                        "log_uri": f"s3://{self.env.name}-emr-logs/",
                        "tags": self.resource_tags
                    }
                }
            }
        }
    
    def generate_terraform_s3_buckets(self) -> Dict:
        """Generate Terraform configuration for S3 buckets"""
        buckets = {
            "data_lake": f"{self.env.name}-data-lake",
            "scripts": f"{self.env.name}-spark-scripts",
            "logs": f"{self.env.name}-emr-logs",
            "artifacts": f"{self.env.name}-artifacts",
            "checkpoint": f"{self.env.name}-checkpoint"
        }
        
        return {
            "resource": {
                "aws_s3_bucket": {
                    f"{self.env.name}_{bucket_type}_bucket": {
                        "bucket": bucket_name,
                        "tags": {
                            **self.resource_tags,
                            "Name": bucket_name,
                            "Purpose": bucket_type
                        }
                    }
                    for bucket_type, bucket_name in buckets.items()
                },
                "aws_s3_bucket_versioning": {
                    f"{self.env.name}_{bucket_type}_versioning": {
                        "bucket": f"${{aws_s3_bucket.{self.env.name}_{bucket_type}_bucket.id}}",
                        "versioning_configuration": {
                            "status": "Enabled" if self.env.env_type == "prod" else "Suspended"
                        }
                    }
                    for bucket_type in buckets.keys()
                },
                "aws_s3_bucket_lifecycle_configuration": {
                    f"{self.env.name}_{bucket_type}_lifecycle": {
                        "bucket": f"${{aws_s3_bucket.{self.env.name}_{bucket_type}_bucket.id}}",
                        "rule": [
                            {
                                "id": "archive_old_data",
                                "status": "Enabled",
                                "transition": [
                                    {
                                        "days": 90,
                                        "storage_class": "STANDARD_IA"
                                    },
                                    {
                                        "days": 180,
                                        "storage_class": "GLACIER"
                                    }
                                ]
                            }
                        ]
                    }
                    for bucket_type in ["data_lake", "logs"]
                }
            }
        }
    
    def _calculate_subnet_cidr(self, index: int) -> str:
        """Calculate subnet CIDR blocks"""
        base = self.env.vpc_cidr.split('/')[0]
        octets = base.split('.')
        octets[2] = str(int(octets[2]) + index)
        return f"{'.'.join(octets)}/24"


# ============================================================================
# DATABRICKS WORKSPACE SETUP
# ============================================================================

class DatabricksWorkspaceProvisioner:
    """Provision Databricks workspace for PySpark migration"""
    
    def __init__(self, env_config: EnvironmentConfig):
        self.env = env_config
    
    def generate_terraform_workspace(self) -> Dict:
        """Generate Terraform configuration for Databricks workspace"""
        return {
            "resource": {
                "databricks_workspace": {
                    f"{self.env.name}_workspace": {
                        "name": f"{self.env.name}-databricks-workspace",
                        "sku": "premium" if self.env.env_type == "prod" else "standard",
                        "location": self.env.region,
                        "managed_resource_group_name": f"{self.env.name}-databricks-rg",
                        "custom_parameters": {
                            "no_public_ip": self.env.env_type == "prod",
                            "virtual_network_id": f"${{azurerm_virtual_network.{self.env.name}_vnet.id}}",
                            "private_subnet_name": f"{self.env.name}-private-subnet",
                            "public_subnet_name": f"{self.env.name}-public-subnet"
                        },
                        "tags": {
                            "Environment": self.env.env_type,
                            "Project": "Informatica-Migration"
                        }
                    }
                },
                "databricks_cluster": {
                    f"{self.env.name}_cluster": {
                        "cluster_name": f"{self.env.name}-spark-cluster",
                        "spark_version": "13.3.x-scala2.12",
                        "node_type_id": "Standard_DS4_v2" if self.env.env_type != "prod" else "Standard_DS5_v2",
                        "autoscale": {
                            "min_workers": 2 if self.env.env_type != "prod" else 5,
                            "max_workers": 8 if self.env.env_type != "prod" else 50
                        },
                        "autotermination_minutes": 30 if self.env.env_type != "prod" else 60,
                        "spark_conf": {
                            "spark.databricks.delta.preview.enabled": "true",
                            "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
                            "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true"
                        },
                        "custom_tags": {
                            "Environment": self.env.env_type,
                            "ManagedBy": "Terraform"
                        }
                    }
                }
            }
        }
    
    def generate_cluster_policies(self) -> Dict:
        """Generate cluster policies for different environments"""
        policies = {
            "dev": {
                "spark_version": {"type": "unlimited"},
                "node_type_id": {"type": "allowlist", "values": ["Standard_DS3_v2", "Standard_DS4_v2"]},
                "autoscale.max_workers": {"type": "range", "maxValue": 10}
            },
            "test": {
                "spark_version": {"type": "unlimited"},
                "node_type_id": {"type": "allowlist", "values": ["Standard_DS4_v2", "Standard_DS5_v2"]},
                "autoscale.max_workers": {"type": "range", "maxValue": 20}
            },
            "prod": {
                "spark_version": {"type": "allowlist", "values": ["13.3.x-scala2.12"]},
                "node_type_id": {"type": "allowlist", "values": ["Standard_DS5_v2", "Standard_DS14_v2"]},
                "autoscale.max_workers": {"type": "range", "maxValue": 100}
            }
        }
        return policies.get(self.env.env_type, policies["dev"])


# ============================================================================
# AIRFLOW ORCHESTRATION SETUP
# ============================================================================

class AirflowOrchestrationSetup:
    """Setup Apache Airflow for workflow orchestration"""
    
    def __init__(self, env_config: EnvironmentConfig):
        self.env = env_config
    
    def generate_docker_compose(self) -> Dict:
        """Generate docker-compose configuration for Airflow"""
        return {
            "version": "3.8",
            "x-airflow-common": {
                "image": "apache/airflow:2.8.0-python3.11",
                "environment": {
                    "AIRFLOW__CORE__EXECUTOR": "CeleryExecutor",
                    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"postgresql+psycopg2://airflow:airflow@postgres/{self.env.name}_airflow",
                    "AIRFLOW__CELERY__RESULT_BACKEND": f"db+postgresql://airflow:airflow@postgres/{self.env.name}_airflow",
                    "AIRFLOW__CELERY__BROKER_URL": "redis://:@redis:6379/0",
                    "AIRFLOW__CORE__FERNET_KEY": "",
                    "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "true",
                    "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
                    "AIRFLOW__API__AUTH_BACKENDS": "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session",
                    "AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK": "true",
                    "_PIP_ADDITIONAL_REQUIREMENTS": "apache-airflow-providers-apache-spark apache-airflow-providers-amazon"
                },
                "volumes": [
                    "./dags:/opt/airflow/dags",
                    "./logs:/opt/airflow/logs",
                    "./plugins:/opt/airflow/plugins",
                    "./config:/opt/airflow/config"
                ],
                "user": "50000:0",
                "depends_on": {
                    "redis": {"condition": "service_healthy"},
                    "postgres": {"condition": "service_healthy"}
                }
            },
            "services": {
                "postgres": {
                    "image": "postgres:15",
                    "environment": {
                        "POSTGRES_USER": "airflow",
                        "POSTGRES_PASSWORD": "airflow",
                        "POSTGRES_DB": f"{self.env.name}_airflow"
                    },
                    "volumes": ["postgres-db-volume:/var/lib/postgresql/data"],
                    "healthcheck": {
                        "test": ["CMD", "pg_isready", "-U", "airflow"],
                        "interval": "10s",
                        "retries": 5,
                        "start_period": "5s"
                    }
                },
                "redis": {
                    "image": "redis:7.2-alpine",
                    "healthcheck": {
                        "test": ["CMD", "redis-cli", "ping"],
                        "interval": "10s",
                        "timeout": "30s",
                        "retries": 50,
                        "start_period": "30s"
                    }
                },
                "airflow-webserver": {
                    "extends": {"service": "x-airflow-common"},
                    "command": "webserver",
                    "ports": ["8080:8080"],
                    "healthcheck": {
                        "test": ["CMD", "curl", "--fail", "http://localhost:8080/health"],
                        "interval": "30s",
                        "timeout": "10s",
                        "retries": 5,
                        "start_period": "30s"
                    }
                },
                "airflow-scheduler": {
                    "extends": {"service": "x-airflow-common"},
                    "command": "scheduler",
                    "healthcheck": {
                        "test": ["CMD", "curl", "--fail", "http://localhost:8974/health"],
                        "interval": "30s",
                        "timeout": "10s",
                        "retries": 5,
                        "start_period": "30s"
                    }
                },
                "airflow-worker": {
                    "extends": {"service": "x-airflow-common"},
                    "command": "celery worker",
                    "healthcheck": {
                        "test": ["CMD-SHELL", "celery --app airflow.executors.celery_executor.app inspect ping -d celery@$HOSTNAME"],
                        "interval": "30s",
                        "timeout": "10s",
                        "retries": 5,
                        "start_period": "30s"
                    }
                },
                "airflow-triggerer": {
                    "extends": {"service": "x-airflow-common"},
                    "command": "triggerer",
                    "healthcheck": {
                        "test": ["CMD-SHELL", "airflow jobs check --job-type TriggererJob --hostname $HOSTNAME"],
                        "interval": "30s",
                        "timeout": "10s",
                        "retries": 5,
                        "start_period": "30s"
                    }
                },
                "airflow-init": {
                    "extends": {"service": "x-airflow-common"},
                    "entrypoint": "/bin/bash",
                    "command": [
                        "-c",
                        "airflow db init && airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin"
                    ]
                }
            },
            "volumes": {
                "postgres-db-volume": None
            }
        }
    
    def generate_airflow_config(self) -> Dict:
        """Generate Airflow configuration"""
        return {
            "core": {
                "dags_folder": "/opt/airflow/dags",
                "load_examples": False,
                "executor": "CeleryExecutor",
                "parallelism": 32 if self.env.env_type == "prod" else 16,
                "max_active_tasks_per_dag": 16 if self.env.env_type == "prod" else 8,
                "max_active_runs_per_dag": 16 if self.env.env_type == "prod" else 4,
                "default_timezone": "UTC"
            },
            "scheduler": {
                "catchup_by_default": False,
                "max_tis_per_query": 512,
                "scheduler_heartbeat_sec": 5,
                "parsing_processes": 4 if self.env.env_type == "prod" else 2
            },
            "webserver": {
                "expose_config": False,
                "rbac": True,
                "default_ui_timezone": "UTC"
            },
            "logging": {
                "remote_logging": True,
                "remote_base_log_folder": f"s3://{self.env.name}-airflow-logs/",
                "remote_log_conn_id": "aws_default",
                "encrypt_s3_logs": True
            }
        }


# ============================================================================
# GIT REPOSITORY STRUCTURE
# ============================================================================

class GitRepositorySetup:
    """Setup Git repository structure and configuration"""
    
    def __init__(self):
        self.repo_structure = {
            "src/": {
                "jobs/": ["__init__.py"],
                "transformations/": ["__init__.py"],
                "utils/": ["__init__.py"],
                "config/": ["__init__.py"]
            },
            "tests/": {
                "unit/": ["__init__.py"],
                "integration/": ["__init__.py"],
                "fixtures/": []
            },
            "dags/": [],
            "notebooks/": {
                "exploration/": [],
                "validation/": []
            },
            "infrastructure/": {
                "terraform/": ["main.tf", "variables.tf", "outputs.tf"],
                "docker/": ["Dockerfile"],
                "kubernetes/": []
            },
            "docs/": ["README.md", "ARCHITECTURE.md", "RUNBOOK.md"],
            "scripts/": ["setup.sh", "deploy.sh"],
            "configs/": {
                "dev/": [],
                "test/": [],
                "prod/": []
            }
        }
    
    def generate_gitignore(self) -> str:
        """Generate .gitignore file content"""
        return """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
build/
dist/
*.egg-info/

# Jupyter Notebooks
.ipynb_checkpoints
*.ipynb

# IDE
.vscode/
.idea/
*.swp
*.swo

# Spark
metastore_db/
derby.log
spark-warehouse/

# Terraform
.terraform/
*.tfstate
*.tfstate.backup
.terraform.lock.hcl

# Airflow
airflow.db
airflow.cfg
logs/
*.pid

# Environment variables
.env
.env.local
*.secret

# OS
.DS_Store
Thumbs.db

# Testing
.pytest_cache/
.coverage
htmlcov/
"""
    
    def generate_github_workflows(self) -> Dict[str, Dict]:
        """Generate GitHub Actions workflows"""
        return {
            "ci.yml": {
                "name": "CI Pipeline",
                "on": {
                    "push": {"branches": ["main", "develop"]},
                    "pull_request": {"branches": ["main", "develop"]}
                },
                "jobs": {
                    "test": {
                        "runs-on": "ubuntu-latest",
                        "steps": [
                            {"uses": "actions/checkout@v3"},
                            {
                                "name": "Set up Python",
                                "uses": "actions/setup-python@v4",
                                "with": {"python-version": "3.11"}
                            },
                            {
                                "name": "Install dependencies",
                                "run": "pip install -r requirements.txt"
                            },
                            {
                                "name": "Run linting",
                                "run": "flake8 src/ tests/"
                            },
                            {
                                "name": "Run type checking",
                                "run": "mypy src/"
                            },
                            {
                                "name": "Run unit tests",
                                "run": "pytest tests/unit/ --cov=src --cov-report=xml"
                            },
                            {
                                "name": "Upload coverage",
                                "uses": "codecov/codecov-action@v3"
                            }
                        ]
                    },
                    "integration-test": {
                        "runs-on": "ubuntu-latest",
                        "needs": "test",
                        "steps": [
                            {"uses": "actions/checkout@v3"},
                            {
                                "name": "Set up Python",
                                "uses": "actions/setup-python@v4",
                                "with": {"python-version": "3.11"}
                            },
                            {
                                "name": "Install dependencies",
                                "run": "pip install -r requirements.txt"
                            },
                            {
                                "name": "Run integration tests",
                                "run": "pytest tests/integration/"
                            }
                        ]
                    }
                }
            },
            "deploy.yml": {
                "name": "Deploy Pipeline",
                "on": {
                    "push": {"branches": ["main"]},
                    "workflow_dispatch": None
                },
                "jobs": {
                    "deploy-dev": {
                        "runs-on": "ubuntu-latest",
                        "environment": "development",
                        "steps": [
                            {"uses": "actions/checkout@v3"},
                            {
                                "name": "Configure AWS credentials",
                                "uses": "aws-actions/configure-aws-credentials@v2",
                                "with": {
                                    "aws-access-key-id": "${{ secrets.AWS_ACCESS_KEY_ID }}",
                                    "aws-secret-access-key": "${{ secrets.AWS_SECRET_ACCESS_KEY }}",
                                    "aws-region": "us-east-1"
                                }
                            },
                            {
                                "name": "Deploy to S3",
                                "run": "aws s3 sync src/ s3://dev-spark-scripts/src/"
                            },
                            {
                                "name": "Trigger Airflow DAG",
                                "run": "curl -X POST http://airflow-dev:8080/api/v1/dags/migration_dag/dagRuns"
                            }
                        ]
                    },
                    "deploy-prod": {
                        "runs-on": "ubuntu-latest",
                        "environment": "production",
                        "needs": "deploy-dev",
                        "steps": [
                            {"uses": "actions/checkout@v3"},
                            {
                                "name": "Configure AWS credentials",
                                "uses": "aws-actions/configure-aws-credentials@v2",
                                "with": {
                                    "aws-access-key-id": "${{ secrets.AWS_ACCESS_KEY_ID }}",
                                    "aws-secret-access-key": "${{ secrets.AWS_SECRET_ACCESS_KEY }}",
                                    "aws-region": "us-east-1"
                                }
                            },
                            {
                                "name": "Deploy to S3",
                                "run": "aws s3 sync src/ s3://prod-spark-scripts/src/"
                            }
                        ]
                    }
                }
            }
        }
    
    def generate_branching_strategy_doc(self) -> str:
        """Generate branching strategy documentation"""
        return """# Git Branching Strategy

## Branch Types

### Main Branches
- **main**: Production-ready code. Protected branch with required reviews.
- **develop**: Integration branch for features. Protected branch.

### Supporting Branches
- **feature/**: New features (feature/TICKET-123-description)
- **bugfix/**: Bug fixes (bugfix/TICKET-123-description)
- **hotfix/**: Production hotfixes (hotfix/TICKET-123-description)
- **release/**: Release preparation (release/v1.0.0)

## Workflow

1. Create feature branch from develop
2. Develop and test locally
3. Submit PR to develop
4. Code review and automated tests
5. Merge to develop after approval
6. Create release branch from develop
7. Deploy to test environment
8. Merge release to main and tag
9. Deploy to production

## Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types: feat, fix, docs, style, refactor, test, chore

## Pull Request Requirements

- All tests passing
- Code coverage > 80%
- At least one approval
- No merge conflicts
- Up to date with base branch
"""


# ============================================================================
# MONITORING AND LOGGING SETUP
# ============================================================================

class MonitoringSetup:
    """Setup monitoring and logging infrastructure"""
    
    def __init__(self, env_config: EnvironmentConfig):
        self.env = env_config
    
    def generate_prometheus_config(self) -> Dict:
        """Generate Prometheus configuration"""
        return {
            "global": {
                "scrape_interval": "15s",
                "evaluation_interval": "15s",
                "external_labels": {
                    "environment": self.env.env_type,
                    "cluster": f"{self.env.name}-spark"
                }
            },
            "alerting": {
                "alertmanagers": [
                    {
                        "static_configs": [
                            {"targets": ["alertmanager:9093"]}
                        ]
                    }
                ]
            },
            "rule_files": [
                "spark_alerts.yml",
                "airflow_alerts.yml"
            ],
            "scrape_configs": [
                {
                    "job_name": "spark-master",
                    "static_configs": [
                        {"targets": ["spark-master:4040"]}
                    ]
                },
                {
                    "job_name": "spark-workers",
                    "static_configs": [
                        {"targets": ["spark-worker:4040"]}
                    ]
                },
                {
                    "job_name": "airflow",
                    "static_configs": [
                        {"targets": ["airflow-webserver:8080"]}
                    ]
                },
                {
                    "job_name": "node-exporter",
                    "static_configs": [
                        {"targets": ["node-exporter:9100"]}
                    ]
                }
            ]
        }
    
    def generate_grafana_dashboards(self) -> Dict[str, Dict]:
        """Generate Grafana dashboard configurations"""
        return {
            "spark_monitoring.json