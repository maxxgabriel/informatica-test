#!/bin/bash
# Script: data_reconciliation.sh
# Purpose: Reconcile source vs target record counts
# Author: ETL Team
# Date: 2026-03-03

echo "====================================="
echo "Data Reconciliation Report"
echo "Generated: $(date)"
echo "====================================="
echo

psql -U ${DB_USER:-etl_user} -d ${DB_NAME:-datawarehouse} <<EOF

\echo '=== STAGING TABLE COUNTS ==='
SELECT 'STG_CUSTOMER' as table_name, COUNT(*) as record_count FROM STG_CUSTOMER
UNION ALL
SELECT 'STG_PRODUCT', COUNT(*) FROM STG_PRODUCT
UNION ALL
SELECT 'STG_SALES', COUNT(*) FROM STG_SALES;

\echo ''
\echo '=== DIMENSION TABLE COUNTS ==='
SELECT 'DIM_CUSTOMER (All)' as table_name, COUNT(*) as record_count FROM DIM_CUSTOMER
UNION ALL
SELECT 'DIM_CUSTOMER (Current)', COUNT(*) FROM DIM_CUSTOMER WHERE IS_CURRENT = 'Y'
UNION ALL
SELECT 'DIM_PRODUCT', COUNT(*) FROM DIM_PRODUCT;

\echo ''
\echo '=== FACT TABLE COUNTS ==='
SELECT 'FACT_SALES' as table_name, COUNT(*) as record_count FROM FACT_SALES;

\echo ''
\echo '=== REFERENTIAL INTEGRITY CHECKS ==='
SELECT 'Orphan Sales (Invalid Customer)' as check_name,
       COUNT(*) as violation_count
FROM FACT_SALES fs
WHERE NOT EXISTS (SELECT 1 FROM DIM_CUSTOMER dc WHERE dc.CUSTOMER_KEY = fs.CUSTOMER_KEY)
UNION ALL
SELECT 'Orphan Sales (Invalid Product)',
       COUNT(*)
FROM FACT_SALES fs
WHERE NOT EXISTS (SELECT 1 FROM DIM_PRODUCT dp WHERE dp.PRODUCT_KEY = fs.PRODUCT_KEY);

\echo ''
\echo '=== DATA QUALITY SUMMARY ==='
SELECT 'Total Customers' as metric, COUNT(*)::text as value FROM DIM_CUSTOMER WHERE IS_CURRENT = 'Y'
UNION ALL
SELECT 'Total Products', COUNT(*)::text FROM DIM_PRODUCT WHERE STATUS = 'ACTIVE'
UNION ALL
SELECT 'Total Sales Transactions', COUNT(*)::text FROM FACT_SALES
UNION ALL
SELECT 'Total Sales Amount', TO_CHAR(SUM(TOTAL_AMOUNT), 'FM\$999,999,999.00') FROM FACT_SALES
UNION ALL
SELECT 'Total Profit', TO_CHAR(SUM(PROFIT_AMOUNT), 'FM\$999,999,999.00') FROM FACT_SALES;

EOF

echo
echo "====================================="
echo "Reconciliation completed"
echo "====================================="

exit 0
