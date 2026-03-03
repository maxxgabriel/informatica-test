#!/bin/bash
###############################################################################
# Script: data_quality_check.sh
# Purpose: Perform data quality checks after ETL load
# Usage: ./data_quality_check.sh
###############################################################################

# Database connection parameters
DB_HOST="datawarehouse.company.com"
DB_PORT="1521"
DB_SID="DWDB"
DB_USER="dw_user"
DB_PASS="<password>"

LOG_FILE="/data/etl/logs/dq_check_$(date +%Y%m%d_%H%M%S).log"
REPORT_FILE="/data/etl/reports/dq_report_$(date +%Y%m%d).html"

# Logging function
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Execute SQL query
execute_sql() {
    local query=$1
    sqlplus -S "$DB_USER/$DB_PASS@//$DB_HOST:$DB_PORT/$DB_SID" <<EOF
SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
$query
EXIT;
EOF
}

log_message "=== Data Quality Check Started ==="

# Initialize counters
TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0

# Array to store failed checks
declare -a FAILED_CHECK_DETAILS

# Check 1: Verify no NULL customer keys in fact table
log_message "Check 1: NULL customer keys in fact table"
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
NULL_CUSTOMERS=$(execute_sql "SELECT COUNT(*) FROM FACT_SALES WHERE CUSTOMER_KEY IS NULL;")
if [ "$NULL_CUSTOMERS" -eq 0 ]; then
    log_message "PASSED: No NULL customer keys found"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
else
    log_message "FAILED: Found $NULL_CUSTOMERS records with NULL customer keys"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    FAILED_CHECK_DETAILS+=("NULL customer keys: $NULL_CUSTOMERS records")
fi

# Check 2: Verify no NULL product keys in fact table
log_message "Check 2: NULL product keys in fact table"
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
NULL_PRODUCTS=$(execute_sql "SELECT COUNT(*) FROM FACT_SALES WHERE PRODUCT_KEY IS NULL;")
if [ "$NULL_PRODUCTS" -eq 0 ]; then
    log_message "PASSED: No NULL product keys found"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
else
    log_message "FAILED: Found $NULL_PRODUCTS records with NULL product keys"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    FAILED_CHECK_DETAILS+=("NULL product keys: $NULL_PRODUCTS records")
fi

# Check 3: Verify no negative quantities
log_message "Check 3: Negative quantities"
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
NEG_QUANTITY=$(execute_sql "SELECT COUNT(*) FROM FACT_SALES WHERE QUANTITY < 0;")
if [ "$NEG_QUANTITY" -eq 0 ]; then
    log_message "PASSED: No negative quantities found"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
else
    log_message "FAILED: Found $NEG_QUANTITY records with negative quantities"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    FAILED_CHECK_DETAILS+=("Negative quantities: $NEG_QUANTITY records")
fi

# Check 4: Verify no negative total amounts
log_message "Check 4: Negative total amounts"
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
NEG_AMOUNT=$(execute_sql "SELECT COUNT(*) FROM FACT_SALES WHERE TOTAL_AMOUNT < 0;")
if [ "$NEG_AMOUNT" -eq 0 ]; then
    log_message "PASSED: No negative amounts found"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
else
    log_message "FAILED: Found $NEG_AMOUNT records with negative amounts"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    FAILED_CHECK_DETAILS+=("Negative amounts: $NEG_AMOUNT records")
fi

# Check 5: Verify referential integrity with customer dimension
log_message "Check 5: Referential integrity - customer dimension"
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
ORPHAN_CUSTOMERS=$(execute_sql "SELECT COUNT(*) FROM FACT_SALES f WHERE NOT EXISTS (SELECT 1 FROM DIM_CUSTOMER c WHERE c.CUSTOMER_KEY = f.CUSTOMER_KEY);")
if [ "$ORPHAN_CUSTOMERS" -eq 0 ]; then
    log_message "PASSED: All customer keys have matching dimension records"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
else
    log_message "FAILED: Found $ORPHAN_CUSTOMERS orphan customer records"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    FAILED_CHECK_DETAILS+=("Orphan customer records: $ORPHAN_CUSTOMERS")
fi

# Check 6: Verify referential integrity with product dimension
log_message "Check 6: Referential integrity - product dimension"
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
ORPHAN_PRODUCTS=$(execute_sql "SELECT COUNT(*) FROM FACT_SALES f WHERE NOT EXISTS (SELECT 1 FROM DIM_PRODUCT p WHERE p.PRODUCT_KEY = f.PRODUCT_KEY);")
if [ "$ORPHAN_PRODUCTS" -eq 0 ]; then
    log_message "PASSED: All product keys have matching dimension records"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
else
    log_message "FAILED: Found $ORPHAN_PRODUCTS orphan product records"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    FAILED_CHECK_DETAILS+=("Orphan product records: $ORPHAN_PRODUCTS")
fi

# Check 7: Verify duplicate transaction IDs
log_message "Check 7: Duplicate transaction IDs"
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
DUPLICATES=$(execute_sql "SELECT COUNT(*) FROM (SELECT TRANSACTION_ID, COUNT(*) FROM FACT_SALES GROUP BY TRANSACTION_ID HAVING COUNT(*) > 1);")
if [ "$DUPLICATES" -eq 0 ]; then
    log_message "PASSED: No duplicate transaction IDs found"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
else
    log_message "FAILED: Found $DUPLICATES duplicate transaction IDs"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    FAILED_CHECK_DETAILS+=("Duplicate transaction IDs: $DUPLICATES")
fi

# Check 8: Verify profit calculations
log_message "Check 8: Profit calculation accuracy"
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
INVALID_PROFIT=$(execute_sql "SELECT COUNT(*) FROM FACT_SALES WHERE ABS((TOTAL_AMOUNT - TAX_AMOUNT - COST_AMOUNT) - PROFIT_AMOUNT) > 0.01;")
if [ "$INVALID_PROFIT" -eq 0 ]; then
    log_message "PASSED: All profit calculations are accurate"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
else
    log_message "FAILED: Found $INVALID_PROFIT records with incorrect profit calculations"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    FAILED_CHECK_DETAILS+=("Incorrect profit calculations: $INVALID_PROFIT records")
fi

# Generate HTML report
cat > "$REPORT_FILE" << EOF
<!DOCTYPE html>
<html>
<head>
    <title>Data Quality Report - $(date +%Y-%m-%d)</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #333; }
        .summary { background: #f0f0f0; padding: 15px; border-radius: 5px; margin: 20px 0; }
        .passed { color: green; }
        .failed { color: red; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #4CAF50; color: white; }
        tr:nth-child(even) { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <h1>Data Quality Report</h1>
    <p>Generated: $(date)</p>

    <div class="summary">
        <h2>Summary</h2>
        <p><strong>Total Checks:</strong> $TOTAL_CHECKS</p>
        <p class="passed"><strong>Passed:</strong> $PASSED_CHECKS</p>
        <p class="failed"><strong>Failed:</strong> $FAILED_CHECKS</p>
        <p><strong>Success Rate:</strong> $(echo "scale=2; $PASSED_CHECKS * 100 / $TOTAL_CHECKS" | bc)%</p>
    </div>

    <h2>Failed Checks</h2>
EOF

if [ "$FAILED_CHECKS" -eq 0 ]; then
    echo "<p class='passed'>All data quality checks passed!</p>" >> "$REPORT_FILE"
else
    echo "<ul>" >> "$REPORT_FILE"
    for detail in "${FAILED_CHECK_DETAILS[@]}"; do
        echo "<li class='failed'>$detail</li>" >> "$REPORT_FILE"
    done
    echo "</ul>" >> "$REPORT_FILE"
fi

cat >> "$REPORT_FILE" << EOF

    <h2>Record Counts</h2>
    <table>
        <tr>
            <th>Table</th>
            <th>Record Count</th>
        </tr>
        <tr>
            <td>DIM_CUSTOMER</td>
            <td>$(execute_sql "SELECT COUNT(*) FROM DIM_CUSTOMER WHERE IS_CURRENT = 'Y';")</td>
        </tr>
        <tr>
            <td>DIM_PRODUCT</td>
            <td>$(execute_sql "SELECT COUNT(*) FROM DIM_PRODUCT;")</td>
        </tr>
        <tr>
            <td>FACT_SALES</td>
            <td>$(execute_sql "SELECT COUNT(*) FROM FACT_SALES;")</td>
        </tr>
        <tr>
            <td>FACT_SALES (Today)</td>
            <td>$(execute_sql "SELECT COUNT(*) FROM FACT_SALES WHERE DATE_KEY = TO_NUMBER(TO_CHAR(SYSDATE, 'YYYYMMDD'));")</td>
        </tr>
    </table>
</body>
</html>
EOF

log_message "HTML report generated: $REPORT_FILE"
log_message "=== Data Quality Check Completed ==="
log_message "Summary: $PASSED_CHECKS passed, $FAILED_CHECKS failed out of $TOTAL_CHECKS checks"

# Exit with error if any checks failed
if [ "$FAILED_CHECKS" -gt 0 ]; then
    exit 1
else
    exit 0
fi
