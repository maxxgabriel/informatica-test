#!/bin/bash
# Script: update_statistics.sh
# Purpose: Update database statistics for specified tables
# Author: ETL Team
# Date: 2026-03-03

TABLES=$@

if [ -z "$TABLES" ]; then
    echo "Usage: $0 <table1> [table2] [table3] ..."
    echo "Example: $0 DIM_CUSTOMER DIM_PRODUCT"
    exit 1
fi

echo "====================================="
echo "Update Statistics Script"
echo "Tables: ${TABLES}"
echo "Started: $(date)"
echo "====================================="

for TABLE in $TABLES; do
    echo "Analyzing ${TABLE}..."
    psql -U ${DB_USER:-etl_user} -d ${DB_NAME:-datawarehouse} -c "ANALYZE ${TABLE};"
    if [ $? -eq 0 ]; then
        echo "${TABLE} statistics updated successfully"
    else
        echo "ERROR: Failed to update statistics for ${TABLE}"
    fi
done

echo "====================================="
echo "Statistics update completed: $(date)"
echo "====================================="

exit 0
