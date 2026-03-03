# Informatica ETL Test Cases

## Test Plan Overview
This document outlines comprehensive test cases for the Informatica ETL sample project.

**Test Environment:** DEV/QA
**Last Updated:** 2026-03-03
**Test Lead:** ETL Team

---

## 1. Staging Load Tests

### Test Case 1.1: Customer Staging Load
**Objective:** Verify customer data loads correctly into staging table

| Test ID | Description | Test Data | Expected Result | Status |
|---------|-------------|-----------|-----------------|--------|
| TC-STG-001 | Load valid customer records | customer_master.csv (100 records) | All 100 records loaded successfully | ✓ Pass |
| TC-STG-002 | Handle duplicate customer IDs | customer_with_duplicates.csv | Duplicates loaded, metadata added | ✓ Pass |
| TC-STG-003 | Load with missing optional fields | customer_partial.csv | Records loaded with NULL values | Pending |
| TC-STG-004 | File not found scenario | non_existent.csv | Session fails with clear error message | Pending |
| TC-STG-005 | Empty file handling | customer_empty.csv | Session succeeds with 0 rows loaded | Pending |

**Prerequisites:**
- Staging table STG_CUSTOMER is truncated
- Source file available in configured directory
- Database connection active

**Test Steps:**
1. Truncate STG_CUSTOMER table
2. Place test file in source directory
3. Execute session s_LOAD_STG_CUSTOMER
4. Verify row counts match
5. Validate LOAD_DATE and SOURCE_SYSTEM fields populated

---

### Test Case 1.2: Product Staging Load
**Objective:** Verify product data loads correctly into staging table

| Test ID | Description | Test Data | Expected Result | Status |
|---------|-------------|-----------|-----------------|--------|
| TC-STG-101 | Load valid product records | product_master.csv (50 records) | All 50 records loaded | ✓ Pass |
| TC-STG-102 | Handle special characters in description | product_special_chars.csv | Special characters preserved | Pending |
| TC-STG-103 | Negative price handling | product_negative_price.csv | Records loaded (validation in dimension) | Pending |
| TC-STG-104 | Very long product names (>100 chars) | product_long_names.csv | Truncated or rejected per config | Pending |

---

### Test Case 1.3: Sales Staging Load
**Objective:** Verify sales transaction data loads correctly

| Test ID | Description | Test Data | Expected Result | Status |
|---------|-------------|-----------|-----------------|--------|
| TC-STG-201 | Load valid sales transactions | sales_transactions_20260301.csv | All valid records loaded | ✓ Pass |
| TC-STG-202 | Filter zero quantity transactions | sales_with_zeros.csv | Zero quantity records excluded | Pending |
| TC-STG-203 | Multiple file pattern matching | sales_transactions_*.csv (3 files) | All files processed, records combined | Pending |
| TC-STG-204 | Future date transactions | sales_future_dates.csv | Records loaded (validation in fact load) | Pending |

---

## 2. Dimension Load Tests

### Test Case 2.1: Customer Dimension SCD Type 2
**Objective:** Verify SCD Type 2 logic for customer dimension

| Test ID | Description | Test Data | Expected Result | Status |
|---------|-------------|-----------|-----------------|--------|
| TC-DIM-001 | Insert new customer | New customer record | New record with IS_CURRENT='Y' | ✓ Pass |
| TC-DIM-002 | No change to existing customer | Unchanged customer | No new record, existing unchanged | ✓ Pass |
| TC-DIM-003 | Update customer address | Changed address | Old record expired, new record inserted | Pending |
| TC-DIM-004 | Update customer email | Changed email | Old record IS_CURRENT='N', new='Y' | Pending |
| TC-DIM-005 | Multiple changes same customer | Multiple attribute changes | Proper versioning maintained | Pending |
| TC-DIM-006 | Surrogate key generation | New customers | Sequential surrogate keys assigned | Pending |

**Validation Queries:**
```sql
-- Verify SCD Type 2 history
SELECT CUSTOMER_ID, FIRST_NAME, ADDRESS, IS_CURRENT,
       EFFECTIVE_FROM_DATE, EFFECTIVE_TO_DATE
FROM DIM_CUSTOMER
WHERE CUSTOMER_ID = 'C001'
ORDER BY EFFECTIVE_FROM_DATE;

-- Count current records
SELECT COUNT(*) FROM DIM_CUSTOMER WHERE IS_CURRENT = 'Y';
```

---

### Test Case 2.2: Product Dimension SCD Type 1
**Objective:** Verify SCD Type 1 logic for product dimension

| Test ID | Description | Test Data | Expected Result | Status |
|---------|-------------|-----------|-----------------|--------|
| TC-DIM-101 | Insert new product | New product record | New record inserted with surrogate key | ✓ Pass |
| TC-DIM-102 | Update product price | Changed unit price | Existing record updated in place | Pending |
| TC-DIM-103 | Update product status | ACTIVE to DISCONTINUED | Status updated, no history preserved | Pending |
| TC-DIM-104 | Product categorization | Various categories | PRICE_RANGE calculated correctly | Pending |
| TC-DIM-105 | Profit margin calculation | Various prices | PROFIT_MARGIN calculated correctly | Pending |

**Validation Queries:**
```sql
-- Verify updates (no history)
SELECT PRODUCT_ID, PRODUCT_NAME, UNIT_PRICE, STATUS, UPDATED_DATE
FROM DIM_PRODUCT
WHERE PRODUCT_ID = 'P001';

-- Verify calculated fields
SELECT PRODUCT_ID, UNIT_PRICE, COST_PRICE, PROFIT_MARGIN, PRICE_RANGE
FROM DIM_PRODUCT
WHERE PROFIT_MARGIN > 50;
```

---

## 3. Fact Table Load Tests

### Test Case 3.1: Sales Fact Load
**Objective:** Verify sales fact table loading with calculations

| Test ID | Description | Test Data | Expected Result | Status |
|---------|-------------|-----------|-----------------|--------|
| TC-FACT-001 | Load valid sales with dimension lookups | Complete sales records | All records loaded with keys | ✓ Pass |
| TC-FACT-002 | Customer key lookup success | Valid customer IDs | Correct customer keys populated | Pending |
| TC-FACT-003 | Product key lookup success | Valid product IDs | Correct product keys populated | Pending |
| TC-FACT-004 | Discount amount calculation | Various discounts | Calculated correctly | Pending |
| TC-FACT-005 | Profit amount calculation | Various costs | PROFIT_AMOUNT = TOTAL - TAX - COST | Pending |
| TC-FACT-006 | Profit margin percentage | Various amounts | Calculated as (PROFIT/REVENUE)*100 | Pending |
| TC-FACT-007 | Filter orphan records | Invalid customer/product IDs | Records rejected to error log | Pending |

**Validation Queries:**
```sql
-- Verify fact table integrity
SELECT COUNT(*)
FROM FACT_SALES fs
WHERE NOT EXISTS (SELECT 1 FROM DIM_CUSTOMER dc WHERE dc.CUSTOMER_KEY = fs.CUSTOMER_KEY)
   OR NOT EXISTS (SELECT 1 FROM DIM_PRODUCT dp WHERE dp.PRODUCT_KEY = fs.PRODUCT_KEY);
-- Should return 0

-- Verify calculations
SELECT TRANSACTION_ID, QUANTITY, UNIT_PRICE, DISCOUNT_PERCENT,
       DISCOUNT_AMOUNT, COST_AMOUNT, PROFIT_AMOUNT, PROFIT_MARGIN_PERCENT
FROM FACT_SALES
WHERE TRANSACTION_ID = 'TXN001';
```

---

## 4. Workflow Tests

### Test Case 4.1: Daily Sales Load Workflow
**Objective:** Verify end-to-end daily incremental load

| Test ID | Description | Test Data | Expected Result | Status |
|---------|-------------|-----------|-----------------|--------|
| TC-WF-001 | Complete successful workflow run | Full dataset | All tasks succeed, email sent | ✓ Pass |
| TC-WF-002 | Pre-process script execution | Valid files | Script completes successfully | Pending |
| TC-WF-003 | Staging failure handling | Corrupted file | Workflow stops, failure email sent | Pending |
| TC-WF-004 | Decision task evaluation | Mixed success/failure | Correct path taken based on condition | Pending |
| TC-WF-005 | Data quality checks | Quality issues | Issues logged, notifications sent | Pending |
| TC-WF-006 | Incremental load using parameters | $$LAST_EXTRACT_DATE | Only new records processed | Pending |

---

### Test Case 4.2: Weekly Dimension Load Workflow
**Objective:** Verify weekly dimension refresh process

| Test ID | Description | Test Data | Expected Result | Status |
|---------|-------------|-----------|-----------------|--------|
| TC-WF-101 | Full dimension refresh | Complete master files | All dimensions refreshed | Pending |
| TC-WF-102 | Backup creation | Existing data | Backup created before load | Pending |
| TC-WF-103 | Archive old data | Expired records | Old versions archived | Pending |
| TC-WF-104 | Index rebuild | All dimension tables | Indexes rebuilt successfully | Pending |
| TC-WF-105 | Statistics update | All dimension tables | Statistics updated | Pending |

---

### Test Case 4.3: Monthly Full Refresh Workflow
**Objective:** Verify complete data warehouse refresh

| Test ID | Description | Test Data | Expected Result | Status |
|---------|-------------|-----------|-----------------|--------|
| TC-WF-201 | Complete full refresh | All source files | Entire DW refreshed | Pending |
| TC-WF-202 | Pre-refresh validation | System checks | Validation passes before proceeding | Pending |
| TC-WF-203 | Full database backup | All tables | Complete backup created | Pending |
| TC-WF-204 | Constraint management | All tables | Constraints disabled/enabled properly | Pending |
| TC-WF-205 | Bulk loading performance | Large datasets | Loads complete within SLA | Pending |
| TC-WF-206 | Reconciliation checks | Source vs target | Counts match, no data loss | Pending |

---

## 5. Performance Tests

### Test Case 5.1: Volume Testing
**Objective:** Verify ETL performance with large data volumes

| Test ID | Description | Volume | Expected Performance | Status |
|---------|-------------|--------|---------------------|--------|
| TC-PERF-001 | Customer dimension load | 1 million records | < 15 minutes | Pending |
| TC-PERF-002 | Product dimension load | 100,000 records | < 5 minutes | Pending |
| TC-PERF-003 | Sales fact load | 10 million records | < 30 minutes | Pending |
| TC-PERF-004 | Full refresh | Complete dataset | < 2 hours | Pending |
| TC-PERF-005 | Parallel processing | Partitioned load | 4x speedup with 4 partitions | Pending |

---

## 6. Data Quality Tests

### Test Case 6.1: Data Validation
**Objective:** Verify data quality rules enforcement

| Test ID | Description | Test Data | Expected Result | Status |
|---------|-------------|-----------|-----------------|--------|
| TC-DQ-001 | NULL handling in required fields | Records with NULLs | Records rejected | Pending |
| TC-DQ-002 | Data type validation | Invalid data types | Type conversion or rejection | Pending |
| TC-DQ-003 | Referential integrity | Orphan records | Filtered to error log | Pending |
| TC-DQ-004 | Business rule validation | Negative quantities | Filtered or corrected | Pending |
| TC-DQ-005 | Duplicate detection | Duplicate transactions | Duplicates removed | Pending |

---

## 7. Error Handling Tests

### Test Case 7.1: Exception Scenarios
**Objective:** Verify proper error handling and recovery

| Test ID | Description | Test Scenario | Expected Result | Status |
|---------|-------------|---------------|-----------------|--------|
| TC-ERR-001 | Database connection failure | Kill DB connection | Clear error message, workflow fails | Pending |
| TC-ERR-002 | File permission issues | Read-only source file | Permission error logged | Pending |
| TC-ERR-003 | Disk space exhaustion | Full disk | Graceful failure, rollback | Pending |
| TC-ERR-004 | Session timeout | Long-running session | Timeout handled properly | Pending |
| TC-ERR-005 | Email notification failure | Invalid email config | Error logged, workflow continues | Pending |

---

## 8. Recovery Tests

### Test Case 8.1: Restart and Recovery
**Objective:** Verify recovery from failures

| Test ID | Description | Test Scenario | Expected Result | Status |
|---------|-------------|---------------|-----------------|--------|
| TC-REC-001 | Workflow restart after failure | Mid-workflow failure | Restart from last checkpoint | Pending |
| TC-REC-002 | Session recovery | Session-level failure | Re-run session successfully | Pending |
| TC-REC-003 | Database rollback | Transaction failure | Proper rollback, no partial data | Pending |
| TC-REC-004 | Backup restoration | Data corruption | Restore from backup successfully | Pending |

---

## Test Execution Summary

### Overall Statistics
- **Total Test Cases:** 65
- **Passed:** 5
- **Pending:** 60
- **Failed:** 0
- **Blocked:** 0

### Test Coverage by Module
| Module | Total | Passed | Pending |
|--------|-------|--------|---------|
| Staging Loads | 13 | 3 | 10 |
| Dimension Loads | 11 | 2 | 9 |
| Fact Loads | 7 | 0 | 7 |
| Workflows | 15 | 0 | 15 |
| Performance | 5 | 0 | 5 |
| Data Quality | 5 | 0 | 5 |
| Error Handling | 5 | 0 | 5 |
| Recovery | 4 | 0 | 4 |

---

## Test Environment Setup

### Required Test Data Files
1. `customer_master.csv` - 100 customer records
2. `product_master.csv` - 50 product records
3. `sales_transactions_20260301.csv` - 1000 sales records
4. Various negative test data files

### Database Setup
```sql
-- Create test data
@scripts/create_test_data.sql

-- Reset test environment
TRUNCATE TABLE STG_CUSTOMER;
TRUNCATE TABLE STG_PRODUCT;
TRUNCATE TABLE STG_SALES;
DELETE FROM DIM_CUSTOMER WHERE SOURCE_SYSTEM = 'TEST';
DELETE FROM DIM_PRODUCT WHERE SOURCE_SYSTEM = 'TEST';
DELETE FROM FACT_SALES WHERE SOURCE_SYSTEM = 'TEST';
```

### Configuration
- Update `parameters/session_params.txt` with test file paths
- Configure test email addresses for notifications
- Set up test database connections

---

## Defect Tracking
Log any defects found during testing in the project's defect tracking system with:
- Test Case ID
- Environment
- Steps to reproduce
- Expected vs Actual results
- Severity and Priority

---

## Sign-off
Test execution requires sign-off from:
- ETL Developer
- QA Lead
- Business Analyst
- DBA

**Next Review Date:** 2026-04-01
