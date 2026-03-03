#!/bin/bash
# Script: archive_dimensions.sh
# Purpose: Archive expired dimension records
# Author: ETL Team
# Date: 2026-03-03

ARCHIVE_DATE=$1

if [ -z "$ARCHIVE_DATE" ]; then
    echo "Usage: $0 <archive_date>"
    echo "Example: $0 2026-01-01"
    exit 1
fi

echo "====================================="
echo "Dimension Archive Script"
echo "Archive Date: ${ARCHIVE_DATE}"
echo "Started: $(date)"
echo "====================================="

# Archive expired customer dimension records
psql -U ${DB_USER:-etl_user} -d ${DB_NAME:-datawarehouse} <<EOF
-- Archive expired customer records
INSERT INTO DIM_CUSTOMER_ARCHIVE
SELECT * FROM DIM_CUSTOMER
WHERE IS_CURRENT = 'N'
  AND EFFECTIVE_TO_DATE < '${ARCHIVE_DATE}'::date
  AND CUSTOMER_KEY NOT IN (SELECT CUSTOMER_KEY FROM DIM_CUSTOMER_ARCHIVE);

-- Log archive activity
INSERT INTO ETL_AUDIT_LOG (process_name, event_date, records_archived, status)
VALUES ('ARCHIVE_DIM_CUSTOMER', CURRENT_TIMESTAMP,
        (SELECT COUNT(*) FROM DIM_CUSTOMER WHERE IS_CURRENT = 'N' AND EFFECTIVE_TO_DATE < '${ARCHIVE_DATE}'::date),
        'SUCCESS');
EOF

echo "Archive completed: $(date)"
exit 0
