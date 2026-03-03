# Deployment Guide - Informatica ETL Project

## Prerequisites

### Software Requirements
- Informatica PowerCenter 10.2 or higher
  - Repository Service
  - Integration Service
  - PowerCenter Client
- Database Server (one of):
  - Oracle 12c or higher
  - SQL Server 2016 or higher
  - PostgreSQL 11 or higher
- Operating System:
  - Linux (RHEL 7/8, CentOS 7/8)
  - Windows Server 2016 or higher

### Hardware Requirements

#### Development Environment
- CPU: 4 cores
- RAM: 16 GB
- Disk: 100 GB free space

#### Production Environment
- CPU: 8+ cores
- RAM: 32+ GB
- Disk: 500 GB free space (SSD recommended)
- Network: 1 Gbps or higher

### Access Requirements
- Database admin credentials
- Informatica repository admin access
- File system write permissions
- SMTP server for email notifications

## Step 1: Database Setup

### 1.1 Create Database Users

```sql
-- Oracle example
CREATE USER dw_stage IDENTIFIED BY <password>;
CREATE USER dw_prod IDENTIFIED BY <password>;
CREATE USER etl_control IDENTIFIED BY <password>;

-- Grant privileges
GRANT CONNECT, RESOURCE TO dw_stage;
GRANT CONNECT, RESOURCE TO dw_prod;
GRANT CONNECT, RESOURCE TO etl_control;

GRANT UNLIMITED TABLESPACE TO dw_stage;
GRANT UNLIMITED TABLESPACE TO dw_prod;
GRANT UNLIMITED TABLESPACE TO etl_control;
```

### 1.2 Execute DDL Scripts

Run the SQL scripts in order:

```bash
cd "sql/"

# Connect as dw_stage user
sqlplus dw_stage/<password>@<database>
@01_create_staging_tables.sql

# Connect as dw_prod user
sqlplus dw_prod/<password>@<database>
@02_create_dimension_tables.sql
@03_create_fact_tables.sql
@04_create_control_tables.sql
```

### 1.3 Populate Date Dimension

```sql
-- Generate dates for 10 years (2020-2030)
DECLARE
    v_date DATE := TO_DATE('2020-01-01', 'YYYY-MM-DD');
    v_end_date DATE := TO_DATE('2030-12-31', 'YYYY-MM-DD');
BEGIN
    WHILE v_date <= v_end_date LOOP
        INSERT INTO DIM_DATE (
            DATE_KEY,
            FULL_DATE,
            DAY_OF_WEEK,
            DAY_NAME,
            DAY_OF_MONTH,
            DAY_OF_YEAR,
            WEEK_OF_YEAR,
            MONTH_NUMBER,
            MONTH_NAME,
            QUARTER_NUMBER,
            YEAR,
            IS_WEEKEND,
            IS_HOLIDAY
        ) VALUES (
            TO_NUMBER(TO_CHAR(v_date, 'YYYYMMDD')),
            v_date,
            TO_NUMBER(TO_CHAR(v_date, 'D')),
            TO_CHAR(v_date, 'Day'),
            TO_NUMBER(TO_CHAR(v_date, 'DD')),
            TO_NUMBER(TO_CHAR(v_date, 'DDD')),
            TO_NUMBER(TO_CHAR(v_date, 'IW')),
            TO_NUMBER(TO_CHAR(v_date, 'MM')),
            TO_CHAR(v_date, 'Month'),
            TO_NUMBER(TO_CHAR(v_date, 'Q')),
            TO_NUMBER(TO_CHAR(v_date, 'YYYY')),
            CASE WHEN TO_CHAR(v_date, 'D') IN ('1', '7') THEN 'Y' ELSE 'N' END,
            'N'
        );

        v_date := v_date + 1;
    END LOOP;
    COMMIT;
END;
/
```

## Step 2: Informatica Repository Setup

### 2.1 Create Repository Folder

```bash
# Using pmrep command
pmrep connect \
  -r Repository_Name \
  -d Domain_Name \
  -n admin \
  -x password

pmrep createfolder \
  -n "SALES_ETL" \
  -d "Sales Data Warehouse ETL Project"
```

### 2.2 Create Database Connections

In PowerCenter Designer or Repository Manager:

1. **Source Staging Connection**
   - Name: `conn_source_staging`
   - Type: Oracle/SQL Server
   - Username: `dw_stage`
   - Connect String: `<host>:<port>/<service_name>`

2. **Target Data Warehouse Connection**
   - Name: `conn_target_dw`
   - Type: Oracle/SQL Server
   - Username: `dw_prod`
   - Connect String: `<host>:<port>/<service_name>`

3. **Control Connection**
   - Name: `conn_etl_control`
   - Type: Oracle/SQL Server
   - Username: `etl_control`
   - Connect String: `<host>:<port>/<service_name>`

## Step 3: Import Informatica Objects

### 3.1 Import Source and Target Definitions

```bash
# Import staging tables
pmrep importobject \
  -i mappings/sources.xml \
  -c conn_source_staging \
  -f SALES_ETL

# Import warehouse tables
pmrep importobject \
  -i mappings/targets.xml \
  -c conn_target_dw \
  -f SALES_ETL
```

### 3.2 Import Mappings

```bash
# Import all mappings
for mapping in mappings/*.xml; do
  pmrep importobject \
    -i "$mapping" \
    -f SALES_ETL
done
```

### 3.3 Import Workflows

```bash
# Import workflows
for workflow in workflows/*.xml; do
  pmrep importobject \
    -i "$workflow" \
    -f SALES_ETL
done
```

## Step 4: Configure File Directories

### 4.1 Create Directory Structure

```bash
# Create directories
sudo mkdir -p /data/etl/{source_files,archive,error,reports,logs}

# Set permissions
sudo chown -R infa_user:infa_group /data/etl
sudo chmod -R 755 /data/etl
```

### 4.2 Configure Directory Variables

In Workflow Manager, set workflow variables:
- `$PMSourceFileDir` = `/data/etl/source_files`
- `$PMBadFileDir` = `/data/etl/error`
- `$PMTargetFileDir` = `/data/etl/reports`
- `$PMLogDir` = `/data/etl/logs`

## Step 5: Configure Sessions

### 5.1 Update Connection Objects

For each session:
1. Open session properties
2. Set source connection to `conn_source_staging`
3. Set target connection to `conn_target_dw`
4. Configure file paths

### 5.2 Configure Session Properties

```properties
# Performance
DTM Buffer Size: 64000000
Buffer Block Size: 128000
Commit Interval: 10000

# Error Handling
Stop on Errors: 0
Error Threshold: 1000

# Pushdown Optimization
Database Pushdown: YES
ELT Enabled: YES

# Partitioning
Partition Count: 4
Partition Type: Hash Auto-Keys
```

## Step 6: Deploy Scripts

### 6.1 Copy Scripts

```bash
# Copy shell scripts
sudo cp scripts/*.sh /data/etl/scripts/
sudo chmod +x /data/etl/scripts/*.sh

# Update paths in scripts if needed
sudo vi /data/etl/scripts/pre_process.sh
# Update SOURCE_DIR, ARCHIVE_DIR, etc.
```

### 6.2 Install Dependencies

```bash
# Install required utilities
sudo yum install -y dos2unix
sudo yum install -y bc
```

## Step 7: Configure Email Notifications

### 7.1 Configure SMTP Settings

In Workflow Manager:
1. Tools → Email Configuration
2. Set SMTP server details
3. Test connection

### 7.2 Update Email Tasks

Update email addresses in workflows:
- Success notifications
- Failure alerts
- Daily reports

## Step 8: Testing

### 8.1 Unit Testing

Test individual sessions:

```bash
# Test staging load
pmcmd startworkflow \
  -sv IntegrationService \
  -d Domain \
  -u admin \
  -p password \
  -f SALES_ETL \
  -workflow wf_TEST_STAGING_LOAD

# Check session logs
tail -f /data/etl/logs/session_*.log
```

### 8.2 Integration Testing

1. Place test files in source directory
2. Run complete workflow
3. Verify data in target tables
4. Check control tables for statistics
5. Review error logs

```sql
-- Verify row counts
SELECT 'FACT_SALES' AS TABLE_NAME, COUNT(*) FROM FACT_SALES
UNION ALL
SELECT 'DIM_CUSTOMER', COUNT(*) FROM DIM_CUSTOMER WHERE IS_CURRENT = 'Y'
UNION ALL
SELECT 'DIM_PRODUCT', COUNT(*) FROM DIM_PRODUCT;

-- Check for errors
SELECT * FROM ETL_ERROR_LOG WHERE ERROR_TIMESTAMP >= SYSDATE - 1;
```

### 8.3 Performance Testing

Run with production-like volumes:

```bash
# Monitor performance
pmcmd getworkflowdetails \
  -sv IntegrationService \
  -d Domain \
  -u admin \
  -p password \
  -f SALES_ETL \
  -workflow wf_DAILY_SALES_LOAD
```

## Step 9: Schedule Workflows

### 9.1 Configure Scheduler

In Workflow Manager:
1. Open workflow properties
2. Go to Scheduler tab
3. Configure schedule:
   - **Daily Sales Load**: Daily at 2:00 AM
   - **Dimension Load**: Weekly on Sunday at 1:00 AM
   - **Full Refresh**: Monthly on 1st at 12:00 AM

### 9.2 Alternative: Use Cron

```bash
# Edit crontab
crontab -e

# Add entries
0 2 * * * /data/etl/scripts/run_daily_load.sh >> /data/etl/logs/cron.log 2>&1
0 1 * * 0 /data/etl/scripts/run_dimension_load.sh >> /data/etl/logs/cron.log 2>&1
0 0 1 * * /data/etl/scripts/run_full_refresh.sh >> /data/etl/logs/cron.log 2>&1
```

## Step 10: Documentation and Handover

### 10.1 Create Runbook

Document:
- Start/stop procedures
- Troubleshooting steps
- Contact information
- Escalation procedures

### 10.2 Knowledge Transfer

Conduct sessions on:
- Architecture overview
- Workflow execution
- Error handling
- Monitoring and maintenance

## Post-Deployment Checklist

- [ ] All database objects created successfully
- [ ] All mappings and workflows imported
- [ ] Database connections tested
- [ ] File directories created and accessible
- [ ] Scripts deployed and executable
- [ ] Email notifications configured
- [ ] Unit tests passed
- [ ] Integration tests passed
- [ ] Performance tests passed
- [ ] Scheduler configured
- [ ] Monitoring setup complete
- [ ] Documentation completed
- [ ] Team training completed
- [ ] Runbook created
- [ ] Emergency contacts documented

## Rollback Plan

If deployment fails:

1. **Database Rollback**
   ```sql
   DROP USER dw_stage CASCADE;
   DROP USER dw_prod CASCADE;
   DROP USER etl_control CASCADE;
   ```

2. **Repository Rollback**
   ```bash
   pmrep deletefolder -n "SALES_ETL"
   ```

3. **File System Cleanup**
   ```bash
   sudo rm -rf /data/etl
   ```

## Support

For issues during deployment:
- Check logs in `/data/etl/logs/`
- Review session logs in Workflow Monitor
- Contact: etl-team@company.com
- Emergency: +1-XXX-XXX-XXXX
