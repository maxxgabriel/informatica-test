#!/bin/bash
# Script: rebuild_indexes.sh
# Purpose: Rebuild indexes on dimension tables
# Author: ETL Team
# Date: 2026-03-03

echo "====================================="
echo "Index Rebuild Script"
echo "Started: $(date)"
echo "====================================="

psql -U ${DB_USER:-etl_user} -d ${DB_NAME:-datawarehouse} <<EOF

-- Rebuild Customer Dimension Indexes
\echo 'Rebuilding DIM_CUSTOMER indexes...'
REINDEX TABLE DIM_CUSTOMER;

-- Rebuild Product Dimension Indexes
\echo 'Rebuilding DIM_PRODUCT indexes...'
REINDEX TABLE DIM_PRODUCT;

-- Log rebuild activity
INSERT INTO ETL_AUDIT_LOG (process_name, event_date, status, comments)
VALUES ('REBUILD_DIMENSION_INDEXES', CURRENT_TIMESTAMP, 'SUCCESS', 'All dimension indexes rebuilt');

\echo 'Index rebuild completed'

EOF

echo "====================================="
echo "Index rebuild completed: $(date)"
echo "====================================="

exit 0
