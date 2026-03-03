# infrastructure/terraform/main.tf
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket         = "pyspark-migration-tfstate"
    key            = "terraform.tfstate"
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
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# S3 Buckets for Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = "pyspark-migration-datalake-${var.environment}"
}

resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_encryption" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# EMR Cluster Configuration
resource "aws_emr_cluster" "spark_cluster" {
  name          = "pyspark-migration-cluster-${var.environment}"
  release_label = "emr-6.15.0"
  applications  = ["Spark", "Hadoop", "Hive", "Livy"]

  ec2_attributes {
    subnet_id                         = aws_subnet.private_subnet.id
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_worker.id
    instance_profile                  = aws_iam_instance_profile.emr_profile.arn
    key_name                          = aws_key_pair.emr_key.key_name
  }

  master_instance_group {
    instance_type  = var.master_instance_type
    instance_count = 1
    ebs_config {
      size                 = 100
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count
    ebs_config {
      size                 = 200
      type                 = "gp3"
      volumes_per_instance = 2
    }
    autoscaling_policy = jsonencode({
      Constraints = {
        MinCapacity = var.core_instance_count
        MaxCapacity = var.core_instance_count * 2
      }
      Rules = [
        {
          Name        = "ScaleUpMemory"
          Description = "Scale up if YARNMemoryAvailablePercentage is less than 15"
          Action = {
            SimpleScalingPolicyConfiguration = {
              AdjustmentType         = "CHANGE_IN_CAPACITY"
              ScalingAdjustment      = 1
              CoolDown               = 300
            }
          }
          Trigger = {
            CloudWatchAlarmDefinition = {
              ComparisonOperator = "LESS_THAN"
              EvaluationPeriods  = 1
              MetricName         = "YARNMemoryAvailablePercentage"
              Namespace          = "AWS/ElasticMapReduce"
              Period             = 300
              Statistic          = "AVERAGE"
              Threshold          = 15.0
              Unit               = "PERCENT"
            }
          }
        }
      ]
    })
  }

  service_role = aws_iam_role.emr_service_role.arn

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.executor.memory"              = "4g"
        "spark.executor.cores"               = "2"
        "spark.driver.memory"                = "4g"
        "spark.dynamicAllocation.enabled"    = "true"
        "spark.sql.adaptive.enabled"         = "true"
        "spark.sql.adaptive.coalescePartitions.enabled" = "true"
        "spark.eventLog.enabled"             = "true"
        "spark.eventLog.dir"                 = "s3://${aws_s3_bucket.data_lake.id}/spark-logs/"
        "spark.history.fs.logDirectory"      = "s3://${aws_s3_bucket.data_lake.id}/spark-logs/"
      }
    },
    {
      Classification = "spark-hive-site"
      Properties = {
        "hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    }
  ])

  log_uri = "s3://${aws_s3_bucket.data_lake.id}/emr-logs/"

  tags = {
    Name = "PySpark Migration Cluster"
  }
}

# VPC Configuration
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
}

resource "aws_subnet" "private_subnet" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "${var.aws_region}a"
}

resource "aws_subnet" "public_subnet" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true
}

# Security Groups
resource "aws_security_group" "emr_master" {
  name        = "emr-master-sg-${var.environment}"
  vpc_id      = aws_vpc.main.id
  description = "Security group for EMR master nodes"

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr_blocks]
  }

  ingress {
    from_port   = 8998
    to_port     = 8998
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr_blocks]
  }
}

resource "aws_security_group" "emr_worker" {
  name        = "emr-worker-sg-${var.environment}"
  vpc_id      = aws_vpc.main.id
  description = "Security group for EMR worker nodes"

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# RDS for Airflow Metadata
resource "aws_db_instance" "airflow_metadata" {
  identifier             = "airflow-metadata-${var.environment}"
  engine                 = "postgres"
  engine_version         = "15.4"
  instance_class         = "db.t3.medium"
  allocated_storage      = 100
  storage_encrypted      = true
  db_name                = "airflow"
  username               = "airflow"
  password               = random_password.airflow_db_password.result
  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.airflow.name
  skip_final_snapshot    = var.environment != "prod"
  backup_retention_period = 7
  multi_az               = var.environment == "prod"
}

resource "aws_db_subnet_group" "airflow" {
  name       = "airflow-subnet-group-${var.environment}"
  subnet_ids = [aws_subnet.private_subnet.id, aws_subnet.public_subnet.id]
}

resource "aws_security_group" "rds" {
  name        = "rds-sg-${var.environment}"
  vpc_id      = aws_vpc.main.id
  description = "Security group for RDS instances"

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.airflow.id]
  }
}

# ECS Cluster for Airflow
resource "aws_ecs_cluster" "airflow" {
  name = "airflow-cluster-${var.environment}"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# IAM Roles
resource "aws_iam_role" "emr_service_role" {
  name = "emr-service-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service_policy" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_ec2_role" {
  name = "emr-ec2-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_instance_profile" "emr_profile" {
  name = "emr-profile-${var.environment}"
  role = aws_iam_role.emr_ec2_role.name
}

resource "aws_iam_role_policy_attachment" "emr_ec2_policy" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

# Glue Catalog Database
resource "aws_glue_catalog_database" "migration_catalog" {
  name = "pyspark_migration_${var.environment}"
  description = "Glue catalog database for PySpark migration"
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "spark_logs" {
  name              = "/aws/emr/spark-${var.environment}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "airflow_logs" {
  name              = "/aws/ecs/airflow-${var.environment}"
  retention_in_days = 30
}

# Secrets Manager
resource "random_password" "airflow_db_password" {
  length  = 32
  special = true
}

resource "aws_secretsmanager_secret" "airflow_db_password" {
  name = "airflow-db-password-${var.environment}"
}

resource "aws_secretsmanager_secret_version" "airflow_db_password" {
  secret_id     = aws_secretsmanager_secret.airflow_db_password.id
  secret_string = random_password.airflow_db_password.result
}

# infrastructure/terraform/variables.tf
variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, test, prod)"
  type        = string
}

variable "master_instance_type" {
  description = "EMR master instance type"
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_type" {
  description = "EMR core instance type"
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_count" {
  description = "Number of core instances"
  type        = number
  default     = 2
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access resources"
  type        = string
}

# infrastructure/terraform/outputs.tf
output "emr_cluster_id" {
  value       = aws_emr_cluster.spark_cluster.id
  description = "EMR cluster ID"
}

output "s3_bucket_name" {
  value       = aws_s3_bucket.data_lake.id
  description = "S3 data lake bucket name"
}

output "glue_catalog_database" {
  value       = aws_glue_catalog_database.migration_catalog.name
  description = "Glue catalog database name"
}

output "airflow_db_endpoint" {
  value       = aws_db_instance.airflow_metadata.endpoint
  description = "Airflow metadata database endpoint"
}

# airflow/docker-compose.yml
version: '3.8'

x-airflow-common:
  &airflow-common
  image: apache/airflow:2.7.3-python3.10
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'
    _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-boto3 apache-airflow-providers-amazon pyspark}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    &airflow-common-depends-on
    redis:
      condition: service_healthy
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s
    restart: always

  redis:
    image: redis:7.2-alpine
    expose:
      - 6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 30s
      retries: 50
      start_period: 30s
    restart: always

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
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8974/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

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
    environment:
      <<: *airflow-common-env
      DUMB_INIT_SETSID: "0"
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-triggerer:
    <<: *airflow-common
    command: triggerer
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        mkdir -p /sources/logs /sources/dags /sources/plugins
        chown -R "${AIRFLOW_UID}:0" /sources/{logs,dags,plugins}
        exec /entrypoint airflow version
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
    user: "0:0"
    volumes:
      - .:/sources

  airflow-cli:
    <<: *airflow-common
    profiles:
      - debug
    environment:
      <<: *airflow-common-env
      CONNECTION_CHECK_MAX_COUNT: "0"
    command:
      - bash
      - -c
      - airflow

volumes:
  postgres-db-volume:

# airflow/dags/sample_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator
import boto3

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

SPARK_STEPS = [
    {
        'Name': 'sample_pyspark_job',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                '--conf', 'spark.executor.memory=4g',
                '--conf', 'spark.executor.cores=2',
                '--conf', 'spark.dynamicAllocation.enabled=true',
                's3://pyspark-migration-datalake-dev/scripts/sample_job.py',
                '--input', 's3://pyspark-migration-datalake-dev/input/',
                '--output', 's3://pyspark-migration-datalake-dev/output/',
            ],
        },
    }
]

with DAG(
    'sample_pyspark_pipeline',
    default_args=default_args,
    description='Sample PySpark migration pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['pyspark', 'migration', 'sample'],
) as dag:

    add_steps = EmrAddStepsOperator(
        task_id='add_spark_steps',
        job_flow_id="{{ var.value.emr_cluster_id }}",
        steps=SPARK_STEPS,
        aws_conn_id='aws_default',
    )

    watch_step = EmrStepSensor(
        task_id='watch_spark_step',
        job_flow_id="{{ var.value.emr_cluster_id }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    add_steps >> watch_step

# config/spark_defaults.py
SPARK_CONFIG = {
    "spark.app.name": "PySpark-Migration",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.shuffle.partitions": "200",
    "spark.executor.memory": "4g",
    "spark.executor.cores": "2",
    "spark.driver.memory": "4g",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "10",
    "spark.dynamicAllocation.executorIdleTimeout": "60s",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.hive.metastorePartitionPruning": "true",
}

# scripts/setup_repo.sh
#!/bin/bash
set -e

echo "Setting up repository structure..."

# Create directory structure
mkdir -p {dags,scripts,config,infrastructure/{terraform,docker},tests/{unit,integration},docs,monitoring,security}

# Initialize git repository
git init

# Create .gitignore
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
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

# Terraform
*.tfstate
*.tfstate.*
.terraform/
.terraform.lock.hcl
crash.log
*.tfvars

# IDE
.vscode/
.idea/
*.swp
*.swo

# Secrets
*.pem
*.key
credentials.json
.env

# Airflow
airflow.db
airflow.cfg
logs/
plugins/__pycache__/

# AWS
.aws/

# Databricks
.databricks/

# Logs
*.log

EOF

# Create README.md
cat > README.md << 'EOF'
# PySpark Migration Project

## Overview
This repository contains the infrastructure, code, and configuration for migrating from Informatica to PySpark.

## Project Structure
```
├── dags/                   # Airflow DAG definitions
├── scripts/                # PySpark job scripts
├── config/                 # Configuration files
├── infrastructure/         # IaC and deployment configs
│   ├── terraform/         # Terraform configurations
│   └── docker/            # Docker configurations
├── tests/                  # Test suites
│   ├── unit/              # Unit tests
│   └── integration/       # Integration tests
├── docs/                   # Documentation
├── monitoring/            # Monitoring and alerting configs
└── security/              # Security policies and configs
```

## Setup Instructions

### Prerequisites
- Python 3.10+
- Terraform 1.5+
- Docker & Docker Compose
- AWS CLI configured
- Git

### Local Development Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd pyspark-migration
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Setup Airflow locally:
```bash
cd airflow
docker-compose up -d
```

4. Access Airflow UI:
```
http://localhost:8080
Username: airflow
Password: airflow
```

### Infrastructure Deployment

1. Initialize Terraform:
```bash
cd infrastructure/terraform
terraform init
```

2. Deploy development environment:
```bash
terraform workspace new dev
terraform plan -var-file=environments/dev.tfvars
terraform apply -var-file=environments/dev.tfvars
```

### Running Tests

```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# Coverage report
pytest --cov=scripts --cov-report=html
```

## CI/CD Pipeline

The project uses GitHub Actions for CI/CD. Pipeline stages:
1. Lint and format check
2. Unit tests
3. Integration tests
4. Security scan
5. Build artifacts
6. Deploy to environment

## Monitoring

- CloudWatch dashboards: [Link to dashboards]
- Grafana: [Link to Grafana]
- PagerDuty alerts: [Link to PagerDuty]

## Security

- All secrets stored in AWS Secrets Manager
- IAM roles follow least privilege principle
- Data encryption at rest and in transit
- VPC with private subnets for compute resources

## Contributing

1. Create feature branch from `develop`
2. Make changes and add tests
3. Submit pull request
4. Ensure CI passes
5. Request code review

## Support

For issues or questions, contact: data-engineering@company.com
EOF

# Create requirements.txt
cat > requirements.txt << 'EOF'
# Core
pyspark==3.5.0
boto3==1.34.0
apache-airflow==2.7.3
apache-airflow-providers-amazon==8.13.0

# Data Processing
pandas==2.1.4
numpy==1.26.2
pyarrow==14.0.1

# Testing
pytest==7.4.3
pytest-cov==4.1.0
pytest-mock==3.12.0
moto==4.2.9

# Linting and Formatting
black==23.12.1
flake8==6.1.0
pylint==3.0.3
mypy==1.7.1

# Development
ipython==8.18.1
jupyter==1.0.0

# Monitoring
prometheus-client==0.19.0

# Security
bandit==1.7.5
safety==2.3.5
EOF

# Create .github/workflows/ci-cd.yml
mkdir -p .github/workflows
cat > .github/workflows/ci-cd.yml << 'EOF'
name: CI/CD Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main, develop]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          pip install black flake8 pylint
      - name: Run linters
        run: |
          black --check scripts/
          flake8 scripts/
          pylint scripts/

  test:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
      - name: Run unit tests
        run: pytest tests/unit/ --cov=scripts --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v3

  security:
    runs-on: ubuntu-latest
    needs: test
    steps:
      - uses: actions/checkout@v3
      - name: Run security scan
        run: |
          pip install bandit safety
          bandit -r scripts/
          safety check

  deploy:
    runs-on: ubuntu-latest
    needs: [test, security]
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v2
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
      - name: Deploy to S3
        run: |
          aws s3 sync scripts/ s3://pyspark-migration-datalake-prod/scripts/
EOF

echo "Repository structure created successfully!"
echo "Next steps:"
echo "1. Configure AWS credentials"
echo "2. Update terraform variables"
echo "3. Run infrastructure setup"
echo "4. Initialize git: git add . && git commit -m 'Initial commit'"

# scripts/spark_session_manager.py
from pyspark.sql import SparkSession
from typing import Dict, Optional
import logging
from config.spark_defaults import SPARK_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkSessionManager:
    """
    Manages SparkSession creation and configuration for PySpark jobs.
    Provides standardized Spark session with best practices.
    """
    
    _instance: Optional[SparkSession] = None
    
    @classmethod
    def get_spark_session(
        cls,
        app_name: str = "PySpark-Migration",
        additional_config: Optional[Dict[str, str]] = None,
        enable_hive: bool = True
    ) -> SparkSession:
        """
        Get or create SparkSession with standard configuration.
        
        Args:
            app_name: Name of the Spark application
            additional_config: Additional Spark configurations to apply
            enable_hive: Whether to enable Hive support