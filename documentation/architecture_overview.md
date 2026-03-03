# Informatica ETL Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                            │
├─────────────────────────────────────────────────────────────────┤
│  • Customer Master (CSV)                                         │
│  • Product Catalog (CSV)                                         │
│  • Sales Transactions (CSV - Daily)                              │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     │ Extract
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    STAGING LAYER (ETL)                           │
├─────────────────────────────────────────────────────────────────┤
│  Informatica PowerCenter                                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Pre-Processing:                                          │   │
│  │  • File validation                                       │   │
│  │  • Format checks                                         │   │
│  │  • Data quality rules                                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Staging Tables:                                          │   │
│  │  • STG_CUSTOMER                                          │   │
│  │  • STG_PRODUCT                                           │   │
│  │  • STG_SALES                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     │ Transform
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                 TRANSFORMATION LAYER                             │
├─────────────────────────────────────────────────────────────────┤
│  • Data Cleansing (trim, upper, standardization)                │
│  • Business Rules (calculations, derivations)                   │
│  • Dimension Lookups (surrogate key assignment)                 │
│  • SCD Implementation (Type 1 & Type 2)                         │
│  • Data Quality Checks (null checks, referential integrity)     │
│  • Aggregations (daily, weekly, monthly)                        │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     │ Load
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   DATA WAREHOUSE (TARGET)                        │
├─────────────────────────────────────────────────────────────────┤
│  Dimensional Model (Star Schema)                                │
│                                                                  │
│  ┌─────────────────┐         ┌─────────────────┐              │
│  │  DIM_CUSTOMER   │         │  DIM_PRODUCT    │              │
│  │  (SCD Type 2)   │         │  (SCD Type 1)   │              │
│  └────────┬────────┘         └────────┬────────┘              │
│           │                           │                        │
│           │        ┌──────────────┐   │                        │
│           └───────►│ FACT_SALES   │◄──┘                        │
│                    │              │                            │
│           ┌───────►│  (Grain:     │◄──┐                        │
│           │        │ Transaction) │   │                        │
│           │        └──────────────┘   │                        │
│  ┌────────┴────────┐         ┌────────┴────────┐              │
│  │   DIM_DATE      │         │   DIM_STORE     │              │
│  │  (Pre-loaded)   │         │   (Future)      │              │
│  └─────────────────┘         └─────────────────┘              │
│                                                                  │
│  Aggregated Facts:                                              │
│  • FACT_SALES_DAILY_AGG                                         │
│  • FACT_SALES_MONTHLY_AGG (Future)                              │
└─────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Source Layer
- **Format**: CSV files
- **Location**: Shared file system / FTP server
- **Frequency**: Daily
- **Volume**: ~10K-100K records per day

### 2. Informatica Components

#### Mappings
- **m_LOAD_STG_CUSTOMER**: Extract customer data to staging
- **m_LOAD_STG_PRODUCT**: Extract product data to staging
- **m_LOAD_STG_SALES**: Extract sales transactions to staging
- **m_LOAD_CUSTOMER_DIM**: Load customer dimension with SCD Type 2
- **m_LOAD_PRODUCT_DIM**: Load product dimension with SCD Type 1
- **m_LOAD_SALES_FACT**: Load sales fact table with calculations
- **m_AGGREGATE_DAILY**: Create daily aggregates

#### Workflows
- **wf_DAILY_SALES_LOAD**: Orchestrates daily incremental load
- **wf_DIMENSION_LOAD**: Weekly dimension refresh
- **wf_FULL_REFRESH**: Monthly full reload

#### Transformations Used
- Source Qualifier
- Expression (data cleansing, calculations)
- Lookup (dimension key retrieval)
- Joiner (combining data)
- Aggregator (summarization)
- Router (conditional routing)
- Update Strategy (insert/update/delete logic)
- Sequence Generator (surrogate keys)
- Filter (data filtering)

### 3. Data Warehouse Layer

#### Dimension Tables
1. **DIM_CUSTOMER** (SCD Type 2)
   - Tracks customer history
   - Effective dates for versioning
   - Current flag for active records

2. **DIM_PRODUCT** (SCD Type 1)
   - Current product information
   - No history tracking

3. **DIM_DATE** (Pre-populated)
   - Calendar attributes
   - Fiscal attributes
   - Holiday flags

#### Fact Tables
1. **FACT_SALES** (Transaction Grain)
   - One row per transaction
   - Detailed measures
   - Foreign keys to dimensions

2. **FACT_SALES_DAILY_AGG** (Daily Grain)
   - Aggregated metrics
   - Optimized for reporting

### 4. Control & Monitoring

#### Control Tables
- **ETL_CONTROL_LOG**: Workflow execution tracking
- **ETL_ERROR_LOG**: Error logging
- **ETL_PERFORMANCE_STATS**: Performance metrics
- **ETL_DATA_QUALITY_LOG**: DQ check results

#### Monitoring Features
- Email notifications (success/failure)
- Session logs
- Performance statistics
- Data quality reports

## Data Flow Sequence

1. **Pre-Processing** (Shell script)
   - File validation
   - Format checks
   - Backup creation

2. **Stage Load** (Parallel sessions)
   - Load all staging tables simultaneously
   - Minimal transformations
   - Fast bulk loading

3. **Dimension Load** (Sequential)
   - Load customer dimension (SCD Type 2)
   - Load product dimension (SCD Type 1)
   - Update dimension keys

4. **Fact Load**
   - Lookup dimension keys
   - Calculate measures
   - Load fact table with partitioning

5. **Post-Processing**
   - Data quality checks
   - Generate reports
   - Send notifications
   - Archive source files

## Performance Optimization

### Strategies Implemented
1. **Partitioning**: 4-way hash partitioning on fact load
2. **Bulk Loading**: Enabled for large tables
3. **Persistent Cache**: Used for dimension lookups
4. **Constraint-Based Loading**: Disabled constraints during load
5. **Parallel Processing**: Multiple sessions run concurrently
6. **Incremental Loading**: CDC-based incremental approach

### Expected Performance
- **Staging Load**: 50K rows/minute
- **Dimension Load**: 30K rows/minute
- **Fact Load**: 100K rows/minute (with partitioning)
- **Total Daily Load**: ~15-20 minutes for 500K records

## Error Handling

### Strategies
1. **Validation at Source**: Pre-process script validates files
2. **Staging Layer**: Accepts all data, marks errors
3. **Transformation Layer**: Filters invalid records
4. **Error Logging**: All errors logged to control tables
5. **Notifications**: Email alerts on failures
6. **Recovery**: Automatic retry for transient errors

## Security

### Access Control
- Separate database users for staging and production
- Read-only access for reporting
- Encrypted passwords in parameter files
- Audit trails for all data changes

### Data Privacy
- No PII in logs or error files
- Masked data in development environments
- Compliance with data retention policies

## Scalability

### Current Capacity
- 1 million transactions per day
- 100K active customers
- 10K products

### Growth Strategy
- Add more partitions as volume grows
- Implement pushdown optimization
- Consider grid deployment for high availability
- Archive historical data to cold storage

## Future Enhancements

1. **Real-time Streaming**: Kafka integration for real-time data
2. **Cloud Migration**: Move to Informatica Cloud (IICS)
3. **Additional Dimensions**: Store, Time, Payment Method
4. **Machine Learning**: Predictive analytics integration
5. **Data Lake**: Integration with Hadoop/Spark ecosystem
