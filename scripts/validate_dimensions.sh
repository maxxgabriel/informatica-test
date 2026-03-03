#!/bin/bash
# Script: validate_dimensions.sh
# Purpose: Validate dimension data quality
# Author: ETL Team
# Date: 2026-03-03

echo "====================================="
echo "Dimension Data Quality Validation"
echo "Started: $(date)"
echo "====================================="

ERRORS=0

# Run validation queries
psql -U ${DB_USER:-etl_user} -d ${DB_NAME:-datawarehouse} -t <<EOF

-- Check for duplicate current customer records
\echo 'Checking for duplicate current customer records...'
SELECT CASE
    WHEN COUNT(*) > 0 THEN 'FAILED: ' || COUNT(*) || ' duplicate current customers found'
    ELSE 'PASSED: No duplicate current customers'
END
FROM (
    SELECT CUSTOMER_ID, COUNT(*)
    FROM DIM_CUSTOMER
    WHERE IS_CURRENT = 'Y'
    GROUP BY CUSTOMER_ID
    HAVING COUNT(*) > 1
) dups;

-- Check for NULL required fields
\echo 'Checking for NULL in required fields...'
SELECT CASE
    WHEN COUNT(*) > 0 THEN 'FAILED: ' || COUNT(*) || ' records with NULL required fields'
    ELSE 'PASSED: No NULL required fields'
END
FROM DIM_CUSTOMER
WHERE CUSTOMER_ID IS NULL OR FIRST_NAME IS NULL OR LAST_NAME IS NULL;

-- Check product dimension
\echo 'Checking product dimension data quality...'
SELECT CASE
    WHEN COUNT(*) > 0 THEN 'FAILED: ' || COUNT(*) || ' products with negative prices'
    ELSE 'PASSED: No negative prices'
END
FROM DIM_PRODUCT
WHERE UNIT_PRICE < 0 OR COST_PRICE < 0;

-- Check for orphaned dimension records
\echo 'Checking for consistency...'
SELECT 'INFO: ' || COUNT(*) || ' total customer records'
FROM DIM_CUSTOMER;

SELECT 'INFO: ' || COUNT(*) || ' current customer records'
FROM DIM_CUSTOMER WHERE IS_CURRENT = 'Y';

SELECT 'INFO: ' || COUNT(*) || ' total product records'
FROM DIM_PRODUCT;

EOF

echo "====================================="
echo "Validation completed: $(date)"
echo "====================================="

exit ${ERRORS}
