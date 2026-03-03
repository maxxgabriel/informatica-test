# Informatica ETL Sample Project

## Overview
This is a sample Informatica PowerCenter ETL project demonstrating data integration from multiple sources into a data warehouse. The project implements a typical sales data warehouse scenario.

## Use Case
**Business Requirement:** Load customer sales transactions from multiple regional CSV files into a centralized data warehouse, performing data quality checks, transformations, and aggregations.

## Project Structure

```
informatica-sample/
├── source_data/          # Sample source CSV files
├── mappings/             # Informatica mapping specifications
├── workflows/            # Workflow and session configurations
├── sql/                  # DDL scripts for source and target tables
├── parameters/           # Parameter files for environment configs
├── documentation/        # Project documentation and design specs
├── scripts/              # Utility scripts
└── target_data/          # Sample target data (for reference)
```

## Components

### 1. Source Systems
- **Customer Master**: Customer demographic data
- **Sales Transactions**: Daily sales transaction files
- **Product Master**: Product catalog information

### 2. ETL Processes

#### Mapping: m_LOAD_CUSTOMER_DIM
- **Purpose**: Load customer dimension table
- **Transformations**:
  - Data cleansing (trim, upper case)
  - Address standardization
  - SCD Type 2 implementation
  - Surrogate key generation

#### Mapping: m_LOAD_PRODUCT_DIM
- **Purpose**: Load product dimension table
- **Transformations**:
  - Product categorization
  - Price standardization
  - SCD Type 1 implementation

#### Mapping: m_LOAD_SALES_FACT
- **Purpose**: Load sales fact table
- **Transformations**:
  - Dimension key lookup
  - Data validation (null checks, referential integrity)
  - Calculated fields (profit, margin)
  - Aggregation by day/product/customer

### 3. Workflows
- **wf_DAILY_SALES_LOAD**: Daily incremental load
- **wf_DIMENSION_LOAD**: Weekly dimension refresh
- **wf_FULL_REFRESH**: Monthly full load

## Setup Instructions

### Prerequisites
- Informatica PowerCenter 10.x or higher
- Database (Oracle/SQL Server/PostgreSQL)
- Repository and Integration Service configured

### Installation Steps

1. **Create Repository Objects**
   ```bash
   # Import folder structure
   pmrep importobjects -i mappings/ -c repository_connection
   ```

2. **Execute DDL Scripts**
   ```sql
   -- Run scripts in order:
   @sql/01_create_staging_tables.sql
   @sql/02_create_dimension_tables.sql
   @sql/03_create_fact_tables.sql
   @sql/04_create_control_tables.sql
   ```

3. **Configure Parameters**
   - Update connection details in `parameters/connection_params.txt`
   - Set file paths in `parameters/session_params.txt`

4. **Deploy Workflows**
   - Import workflows from `workflows/` directory
   - Configure session properties
   - Set up scheduling

## Running the ETL

### Manual Execution
```bash
# Start workflow
pmcmd startworkflow -sv IntegrationService -d Domain -u admin -p password -f FolderName -workflow wf_DAILY_SALES_LOAD

# Monitor workflow
pmcmd getworkflowdetails -sv IntegrationService -d Domain -u admin -p password -f FolderName -workflow wf_DAILY_SALES_LOAD
```

### Scheduled Execution
Workflows are configured to run:
- **wf_DAILY_SALES_LOAD**: Daily at 2:00 AM
- **wf_DIMENSION_LOAD**: Every Sunday at 1:00 AM
- **wf_FULL_REFRESH**: First day of month at 12:00 AM

## Data Flow

```
Source Files (CSV)
    ↓
[Extract] → Source Qualifier
    ↓
[Transform] → Expression, Lookup, Joiner, Aggregator
    ↓
[Load] → Target Tables (Data Warehouse)
    ↓
[Post-Load] → Update Control Tables, Send Notifications
```

## Error Handling
- Invalid records logged to `ERROR_LOG` table
- Email notifications on workflow failure
- Automatic retry for transient errors
- Data quality reports generated daily

## Performance Optimization
- Partitioning enabled on large mappings
- Bulk loading mode for fact tables
- Incremental loads using CDC (Change Data Capture)
- Indexes on lookup tables
- Constraint-based loading

## Monitoring & Logging
- Workflow logs: `$PMLogDir/workflow_logs/`
- Session logs: `$PMLogDir/session_logs/`
- Error logs: Database table `ETL_ERROR_LOG`
- Performance stats: `ETL_PERFORMANCE_STATS`

## Testing
Test cases are documented in `documentation/test_cases.md`

## Contact
For questions or issues, contact the Data Integration Team.

## Version History
- **v1.0** (2026-03-03): Initial project setup
