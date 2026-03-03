# =============================================================================
# PySpark Development Environment and Infrastructure Setup
# =============================================================================
# Description: Complete infrastructure setup for Informatica to PySpark migration
# Author: Data Engineering Team
# Version: 1.0.0
# =============================================================================

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import subprocess
import yaml

# =============================================================================
# Configuration Classes
# =============================================================================

class InfrastructureConfig:
    """Configuration for infrastructure setup"""
    
    def __init__(self, cloud_provider: str = "aws"):
        self.cloud_provider = cloud_provider
        self.project_name = "pyspark-migration"
        self.environment = os.getenv("ENVIRONMENT", "dev")
        self.region = os.getenv("CLOUD_REGION", "us-east-1")
        self.setup_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def get_config(self) -> Dict:
        return {
            "cloud_provider": self.cloud_provider,
            "project_name": self.project_name,
            "environment": self.environment,
            "region": self.region,
            "setup_timestamp": self.setup_timestamp
        }


# =============================================================================
# 1. Cloud Infrastructure Provisioning (Terraform)
# =============================================================================

TERRAFORM_MAIN_TF = """
# Terraform Configuration for PySpark Migration Infrastructure
terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  backend "s3" {
    bucket         = "pyspark-migration-terraform-state"
    key            = "infrastructure/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "PySpark-Migration"
      ManagedBy   = "Terraform"
      Environment = var.environment
    }
  }
}

# Variables
variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "pyspark-migration"
}

# VPC Configuration
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  
  tags = {
    Name = "${var.project_name}-vpc-${var.environment}"
  }
}

# Subnets
resource "aws_subnet" "private" {
  count             = 3
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 1}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]
  
  tags = {
    Name = "${var.project_name}-private-subnet-${count.index + 1}-${var.environment}"
  }
}

resource "aws_subnet" "public" {
  count                   = 3
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.${count.index + 101}.0/24"
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  
  tags = {
    Name = "${var.project_name}-public-subnet-${count.index + 1}-${var.environment}"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  
  tags = {
    Name = "${var.project_name}-igw-${var.environment}"
  }
}

# S3 Buckets
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-data-lake-${var.environment}"
  
  tags = {
    Name        = "Data Lake"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "${var.project_name}-artifacts-${var.environment}"
  
  tags = {
    Name        = "Artifacts"
    Environment = var.environment
  }
}

resource "aws_s3_bucket" "logs" {
  bucket = "${var.project_name}-logs-${var.environment}"
  
  tags = {
    Name        = "Logs"
    Environment = var.environment
  }
}

# EMR Cluster Security Groups
resource "aws_security_group" "emr_master" {
  name        = "${var.project_name}-emr-master-${var.environment}"
  description = "Security group for EMR master node"
  vpc_id      = aws_vpc.main.id
  
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  
  ingress {
    from_port   = 8998
    to_port     = 8998
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = {
    Name = "${var.project_name}-emr-master-sg-${var.environment}"
  }
}

# RDS PostgreSQL for Metadata
resource "aws_db_instance" "metadata" {
  identifier             = "${var.project_name}-metadata-${var.environment}"
  engine                 = "postgres"
  engine_version         = "15.3"
  instance_class         = "db.t3.medium"
  allocated_storage      = 100
  storage_encrypted      = true
  db_name                = "metadata"
  username               = "admin"
  manage_master_user_password = true
  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name
  skip_final_snapshot    = var.environment == "dev" ? true : false
  backup_retention_period = 7
  
  tags = {
    Name = "${var.project_name}-metadata-db-${var.environment}"
  }
}

resource "aws_security_group" "rds" {
  name        = "${var.project_name}-rds-${var.environment}"
  description = "Security group for RDS"
  vpc_id      = aws_vpc.main.id
  
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-db-subnet-${var.environment}"
  subnet_ids = aws_subnet.private[*].id
  
  tags = {
    Name = "${var.project_name}-db-subnet-group-${var.environment}"
  }
}

# Data sources
data "aws_availability_zones" "available" {
  state = "available"
}

# Outputs
output "vpc_id" {
  value = aws_vpc.main.id
}

output "data_lake_bucket" {
  value = aws_s3_bucket.data_lake.bucket
}

output "artifacts_bucket" {
  value = aws_s3_bucket.artifacts.bucket
}

output "metadata_db_endpoint" {
  value = aws_db_instance.metadata.endpoint
}
"""

# =============================================================================
# 2. Spark Cluster Configuration (EMR)
# =============================================================================

EMR_CLUSTER_CONFIG = """
{
  "Name": "pyspark-migration-cluster",
  "ReleaseLabel": "emr-6.14.0",
  "Applications": [
    {"Name": "Spark"},
    {"Name": "Hadoop"},
    {"Name": "Hive"},
    {"Name": "Livy"},
    {"Name": "JupyterEnterpriseGateway"}
  ],
  "Configurations": [
    {
      "Classification": "spark-defaults",
      "Properties": {
        "spark.executor.memory": "8G",
        "spark.executor.cores": "4",
        "spark.driver.memory": "4G",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.shuffle.service.enabled": "true",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "s3://pyspark-migration-logs-dev/spark-events/"
      }
    },
    {
      "Classification": "spark-hive-site",
      "Properties": {
        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    }
  ],
  "Instances": {
    "InstanceGroups": [
      {
        "Name": "Master",
        "Market": "ON_DEMAND",
        "InstanceRole": "MASTER",
        "InstanceType": "m5.xlarge",
        "InstanceCount": 1
      },
      {
        "Name": "Core",
        "Market": "ON_DEMAND",
        "InstanceRole": "CORE",
        "InstanceType": "m5.2xlarge",
        "InstanceCount": 2
      },
      {
        "Name": "Task",
        "Market": "SPOT",
        "InstanceRole": "TASK",
        "InstanceType": "m5.xlarge",
        "InstanceCount": 2,
        "BidPrice": "OnDemandPrice"
      }
    ],
    "Ec2KeyName": "emr-key-pair",
    "KeepJobFlowAliveWhenNoSteps": true,
    "TerminationProtected": false
  },
  "BootstrapActions": [
    {
      "Name": "Install Python Dependencies",
      "ScriptBootstrapAction": {
        "Path": "s3://pyspark-migration-artifacts-dev/bootstrap/install_dependencies.sh"
      }
    }
  ],
  "LogUri": "s3://pyspark-migration-logs-dev/cluster-logs/",
  "JobFlowRole": "EMR_EC2_DefaultRole",
  "ServiceRole": "EMR_DefaultRole",
  "VisibleToAllUsers": true,
  "Tags": [
    {"Key": "Environment", "Value": "dev"},
    {"Key": "Project", "Value": "pyspark-migration"}
  ]
}
"""

BOOTSTRAP_SCRIPT = """#!/bin/bash
# EMR Bootstrap Script for PySpark Migration

set -e

# Update system packages
sudo yum update -y

# Install Python 3.10
sudo yum install -y python3.10 python3.10-pip

# Set Python 3.10 as default
sudo alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1

# Upgrade pip
python3 -m pip install --upgrade pip

# Install required Python packages
python3 -m pip install \\
    pyspark==3.5.0 \\
    pandas==2.0.3 \\
    pyarrow==13.0.0 \\
    delta-spark==3.0.0 \\
    boto3==1.28.0 \\
    pyyaml==6.0.1 \\
    pytest==7.4.0 \\
    pytest-cov==4.1.0 \\
    great-expectations==0.17.0 \\
    sqlalchemy==2.0.20 \\
    psycopg2-binary==2.9.7

# Install monitoring tools
python3 -m pip install \\
    prometheus-client==0.17.1 \\
    py4j==0.10.9.7

# Create necessary directories
sudo mkdir -p /mnt/logs/spark
sudo mkdir -p /mnt/tmp
sudo chmod 777 /mnt/logs/spark
sudo chmod 777 /mnt/tmp

# Configure environment variables
cat >> ~/.bashrc << 'EOF'
export SPARK_HOME=/usr/lib/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
EOF

echo "Bootstrap completed successfully"
"""

# =============================================================================
# 3. Databricks Workspace Configuration (Alternative)
# =============================================================================

DATABRICKS_TERRAFORM = """
# Databricks Workspace Configuration

terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.28"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# Workspace Configuration
resource "databricks_cluster" "migration_cluster" {
  cluster_name            = "pyspark-migration-cluster"
  spark_version           = "13.3.x-scala2.12"
  node_type_id            = "i3.xlarge"
  driver_node_type_id     = "i3.xlarge"
  autotermination_minutes = 120
  autoscale {
    min_workers = 2
    max_workers = 8
  }
  
  spark_conf = {
    "spark.sql.adaptive.enabled"                     = "true"
    "spark.databricks.delta.preview.enabled"         = "true"
    "spark.databricks.delta.optimizeWrite.enabled"   = "true"
    "spark.databricks.delta.autoCompact.enabled"     = "true"
  }
  
  aws_attributes {
    availability            = "SPOT_WITH_FALLBACK"
    zone_id                 = "us-east-1a"
    first_on_demand         = 1
    spot_bid_price_percent  = 100
  }
  
  library {
    pypi {
      package = "delta-spark==3.0.0"
    }
  }
  
  library {
    pypi {
      package = "great-expectations==0.17.0"
    }
  }
}

# Create Workspace Directories
resource "databricks_directory" "migration_root" {
  path = "/Workspace/migration"
}

resource "databricks_directory" "notebooks" {
  path = "${databricks_directory.migration_root.path}/notebooks"
}

resource "databricks_directory" "libraries" {
  path = "${databricks_directory.migration_root.path}/libraries"
}

# Secret Scopes
resource "databricks_secret_scope" "migration_secrets" {
  name = "migration-secrets"
}

# Mount S3 Buckets
resource "databricks_mount" "data_lake" {
  name = "data-lake"
  
  s3 {
    bucket_name = var.data_lake_bucket
    instance_profile = aws_iam_instance_profile.databricks.arn
  }
}
"""

# =============================================================================
# 4. Apache Airflow Configuration
# =============================================================================

AIRFLOW_DOCKER_COMPOSE = """
version: '3.8'

x-airflow-common: &airflow-common
  image: apache/airflow:2.7.0-python3.10
  environment:
    - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
    - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
    - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
    - AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
    - AIRFLOW__CORE__FERNET_KEY=''
    - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
    - AIRFLOW__CORE__LOAD_EXAMPLES=false
    - AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
    - AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true
    - AWS_DEFAULT_REGION=us-east-1
    - SPARK_HOME=/usr/local/spark
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    redis:
      condition: service_healthy
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:15
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s
    ports:
      - "5432:5432"

  redis:
    image: redis:7.2
    expose:
      - 6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 30s
      retries: 50
      start_period: 30s

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8974/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s

  airflow-worker:
    <<: *airflow-common
    command: celery worker
    healthcheck:
      test:
        - "CMD-SHELL"
        - 'celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"'
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        mkdir -p /sources/logs /sources/dags /sources/plugins
        chown -R "${AIRFLOW_UID}:0" /sources/{logs,dags,plugins}
        exec /entrypoint airflow version

  flower:
    <<: *airflow-common
    command: celery flower
    ports:
      - "5555:5555"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:5555/"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s

volumes:
  postgres-db-volume:
"""

AIRFLOW_DAG_SAMPLE = """
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Spark job configuration
SPARK_STEPS = [
    {
        'Name': 'Sample PySpark Job',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                '--conf', 'spark.sql.adaptive.enabled=true',
                's3://pyspark-migration-artifacts-dev/jobs/sample_job.py',
                '--date', '{{ ds }}',
                '--env', 'dev'
            ]
        }
    }
]

# DAG definition
with DAG(
    'pyspark_migration_sample',
    default_args=default_args,
    description='Sample PySpark migration DAG',
    schedule_interval='0 6 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['pyspark', 'migration', 'sample'],
) as dag:
    
    # Task to add Spark job step
    add_spark_step = EmrAddStepsOperator(
        task_id='add_spark_step',
        job_flow_id="{{ var.value.emr_cluster_id }}",
        steps=SPARK_STEPS,
        aws_conn_id='aws_default',
    )
    
    # Sensor to wait for step completion
    watch_spark_step = EmrStepSensor(
        task_id='watch_spark_step',
        job_flow_id="{{ var.value.emr_cluster_id }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )
    
    # Define task dependencies
    add_spark_step >> watch_spark_step
"""

# =============================================================================
# 5. Git Repository Structure
# =============================================================================

GIT_IGNORE = """
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
env/
ENV/
.venv

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# Jupyter Notebook
.ipynb_checkpoints

# Environment variables
.env
.env.local
*.env

# Terraform
*.tfstate
*.tfstate.*
.terraform/
.terraform.lock.hcl

# Logs
*.log
logs/

# OS
.DS_Store
Thumbs.db

# Test coverage
htmlcov/
.coverage
.pytest_cache/

# Spark
metastore_db/
spark-warehouse/
derby.log
"""

REPO_STRUCTURE = """
pyspark-migration/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       ├── cd-dev.yml
│       ├── cd-prod.yml
│       └── quality-checks.yml
├── terraform/
│   ├── environments/
│   │   ├── dev/
│   │   ├── staging/
│   │   └── prod/
│   ├── modules/
│   │   ├── networking/
│   │   ├── emr/
│   │   ├── storage/
│   │   └── databases/
│   └── main.tf
├── airflow/
│   ├── dags/
│   ├── plugins/
│   ├── config/
│   └── docker-compose.yml
├── src/
│   ├── jobs/
│   │   ├── ingestion/
│   │   ├── transformation/
│   │   └── aggregation/
│   ├── common/
│   │   ├── config/
│   │   ├── utils/
│   │   ├── schemas/
│   │   └── validators/
│   └── __init__.py
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
├── notebooks/
│   ├── exploration/
│   └── validation/
├── config/
│   ├── dev/
│   ├── staging/
│   └── prod/
├── scripts/
│   ├── deployment/
│   ├── monitoring/
│   └── utilities/
├── docs/
│   ├── architecture/
│   ├── runbooks/
│   └── api/
├── requirements.txt
├── setup.py
├── pytest.ini
├── .gitignore
├── .pre-commit-config.yaml
└── README.md
"""

# =============================================================================
# 6. CI/CD Pipeline Configuration (GitHub Actions)
# =============================================================================

GITHUB_ACTIONS_CI = """
name: CI Pipeline

on:
  push:
    branches: [ develop, main ]
  pull_request:
    branches: [ develop, main ]

env:
  PYTHON_VERSION: '3.10'
  SPARK_VERSION: '3.5.0'

jobs:
  code-quality:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install flake8 black isort mypy pylint
          pip install -r requirements.txt
      
      - name: Run Black
        run: black --check src/ tests/
      
      - name: Run isort
        run: isort --check-only src/ tests/
      
      - name: Run Flake8
        run: flake8 src/ tests/ --max-line-length=100
      
      - name: Run Pylint
        run: pylint src/ --rcfile=.pylintrc
      
      - name: Run MyPy
        run: mypy src/ --ignore-missing-imports

  unit-tests:
    runs-on: ubuntu-latest
    needs: code-quality
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      
      - name: Set up Java
        uses: actions/setup-java@v3
        with:
          distribution: 'temurin'
          java-version: '11'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
          pip install pytest pytest-cov pytest-spark
      
      - name: Run unit tests
        run: |
          pytest tests/unit/ --cov=src --cov-report=xml --cov-report=html
      
      - name: Upload coverage reports
        uses: codecov/codecov-action@v3
        with:
          file: ./coverage.xml
          flags: unittests
          name: codecov-umbrella

  integration-tests:
    runs-on: ubuntu-latest
    needs: unit-tests
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_USER: test
          POSTGRES_PASSWORD: test
          POSTGRES_DB: testdb
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 5432:5432
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
      
      - name: Run integration tests
        run: pytest tests/integration/ -v
        env:
          DB_HOST: localhost
          DB_PORT: 5432
          DB_NAME: testdb
          DB_USER: test
          DB_PASSWORD: test

  build-artifact:
    runs-on: ubuntu-latest
    needs: [unit-tests, integration-tests]
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      
      - name: Build wheel
        run: |
          python -m pip install --upgrade pip build
          python -m build
      
      - name: Upload artifact
        uses: actions/upload-artifact@v3
        with:
          name: pyspark-migration-package
          path: dist/
"""

GITHUB_ACTIONS_CD = """
name: CD Pipeline - Deploy to Dev

on:
  push:
    branches: [ develop ]
  workflow_dispatch:

env:
  AWS_REGION: us-east-1
  ENVIRONMENT: dev

jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      id-token: write
      contents: read
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v2
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_ARN }}
          aws-region: ${{ env.AWS_REGION }}
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Build package
        run: |
          python -m pip install --upgrade pip build
          python -m build
      
      - name: Upload to S3
        run: |
          aws s3 cp dist/ s3://pyspark-migration-artifacts-${{ env.ENVIRONMENT }}/packages/