#!/bin/bash
# Script: backup_dimensions.sh
# Purpose: Backup dimension tables before weekly refresh
# Author: ETL Team
# Date: 2026-03-03

# Configuration
BACKUP_DIR="${BACKUP_DIR:-/backup/informatica}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DB_USER="${DB_USER:-etl_user}"
DB_NAME="${DB_NAME:-datawarehouse}"

echo "====================================="
echo "Dimension Backup Script"
echo "Started: $(date)"
echo "====================================="

# Create backup directory if not exists
mkdir -p "${BACKUP_DIR}/${TIMESTAMP}"

# Backup Customer Dimension
echo "Backing up DIM_CUSTOMER..."
pg_dump -U ${DB_USER} -d ${DB_NAME} -t dim_customer -F c -f "${BACKUP_DIR}/${TIMESTAMP}/dim_customer_${TIMESTAMP}.backup"
if [ $? -eq 0 ]; then
    echo "DIM_CUSTOMER backup completed successfully"
else
    echo "ERROR: DIM_CUSTOMER backup failed"
    exit 1
fi

# Backup Product Dimension
echo "Backing up DIM_PRODUCT..."
pg_dump -U ${DB_USER} -d ${DB_NAME} -t dim_product -F c -f "${BACKUP_DIR}/${TIMESTAMP}/dim_product_${TIMESTAMP}.backup"
if [ $? -eq 0 ]; then
    echo "DIM_PRODUCT backup completed successfully"
else
    echo "ERROR: DIM_PRODUCT backup failed"
    exit 1
fi

# Create backup manifest
cat > "${BACKUP_DIR}/${TIMESTAMP}/manifest.txt" <<EOF
Backup Timestamp: ${TIMESTAMP}
Database: ${DB_NAME}
Tables: DIM_CUSTOMER, DIM_PRODUCT
Backup Location: ${BACKUP_DIR}/${TIMESTAMP}
Created By: $(whoami)
EOF

echo "====================================="
echo "Backup completed successfully"
echo "Location: ${BACKUP_DIR}/${TIMESTAMP}"
echo "Ended: $(date)"
echo "====================================="

exit 0
