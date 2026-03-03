#!/bin/bash
###############################################################################
# Script: pre_process.sh
# Purpose: Pre-process source files before ETL load
# Usage: ./pre_process.sh <source_directory>
###############################################################################

# Set variables
SOURCE_DIR=$1
ARCHIVE_DIR="/data/etl/archive/$(date +%Y%m%d)"
ERROR_DIR="/data/etl/error"
LOG_FILE="/data/etl/logs/pre_process_$(date +%Y%m%d_%H%M%S).log"

# Create directories if they don't exist
mkdir -p "$ARCHIVE_DIR"
mkdir -p "$ERROR_DIR"
mkdir -p "$(dirname "$LOG_FILE")"

# Logging function
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

log_message "=== Pre-processing started ==="
log_message "Source Directory: $SOURCE_DIR"

# Validate source directory
if [ ! -d "$SOURCE_DIR" ]; then
    log_message "ERROR: Source directory does not exist: $SOURCE_DIR"
    exit 1
fi

# Check for required files
REQUIRED_FILES=("customer_master.csv" "product_master.csv")
SALES_FILES=$(find "$SOURCE_DIR" -name "sales_transactions_*.csv" 2>/dev/null)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$SOURCE_DIR/$file" ]; then
        log_message "ERROR: Required file missing: $file"
        exit 1
    fi
done

if [ -z "$SALES_FILES" ]; then
    log_message "ERROR: No sales transaction files found"
    exit 1
fi

# Validate file formats
validate_csv() {
    local file=$1
    local expected_columns=$2

    log_message "Validating file: $(basename "$file")"

    # Check if file is empty
    if [ ! -s "$file" ]; then
        log_message "ERROR: File is empty: $file"
        return 1
    fi

    # Count columns in header
    header_cols=$(head -1 "$file" | awk -F',' '{print NF}')

    if [ "$header_cols" -ne "$expected_columns" ]; then
        log_message "ERROR: Invalid column count in $file. Expected: $expected_columns, Found: $header_cols"
        return 1
    fi

    # Count total rows (excluding header)
    row_count=$(tail -n +2 "$file" | wc -l)
    log_message "File validation passed. Row count: $row_count"

    return 0
}

# Validate each file
validate_csv "$SOURCE_DIR/customer_master.csv" 12
if [ $? -ne 0 ]; then exit 1; fi

validate_csv "$SOURCE_DIR/product_master.csv" 10
if [ $? -ne 0 ]; then exit 1; fi

for sales_file in $SALES_FILES; do
    validate_csv "$sales_file" 12
    if [ $? -ne 0 ]; then exit 1; fi
done

# Data quality checks
log_message "Running data quality checks..."

# Check for duplicate transaction IDs
DUPLICATES=$(tail -n +2 "$SOURCE_DIR"/sales_transactions_*.csv | cut -d',' -f1 | sort | uniq -d)
if [ -n "$DUPLICATES" ]; then
    log_message "WARNING: Duplicate transaction IDs found:"
    echo "$DUPLICATES" | tee -a "$LOG_FILE"
fi

# Check for invalid dates
INVALID_DATES=$(tail -n +2 "$SOURCE_DIR"/sales_transactions_*.csv | awk -F',' '{print $2}' | grep -v '^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}')
if [ -n "$INVALID_DATES" ]; then
    log_message "ERROR: Invalid date formats found"
    exit 1
fi

# Convert files to UTF-8 if needed
log_message "Checking file encodings..."
for file in "$SOURCE_DIR"/*.csv; do
    encoding=$(file -bi "$file" | sed 's/.*charset=//')
    if [ "$encoding" != "utf-8" ] && [ "$encoding" != "us-ascii" ]; then
        log_message "Converting $file from $encoding to UTF-8"
        iconv -f "$encoding" -t UTF-8 "$file" -o "${file}.tmp"
        mv "${file}.tmp" "$file"
    fi
done

# Remove DOS line endings if present
log_message "Normalizing line endings..."
for file in "$SOURCE_DIR"/*.csv; do
    dos2unix "$file" 2>/dev/null || sed -i 's/\r$//' "$file"
done

# Create backup before processing
log_message "Creating backup files..."
for file in "$SOURCE_DIR"/*.csv; do
    cp "$file" "$ARCHIVE_DIR/$(basename "$file")"
done

# Generate pre-processing summary
SUMMARY_FILE="$SOURCE_DIR/pre_process_summary.txt"
cat > "$SUMMARY_FILE" << EOF
Pre-processing Summary
=====================
Date: $(date)
Source Directory: $SOURCE_DIR
Archive Directory: $ARCHIVE_DIR

Files Processed:
$(ls -lh "$SOURCE_DIR"/*.csv | awk '{print $9, $5}')

Total Records:
- Customers: $(tail -n +2 "$SOURCE_DIR/customer_master.csv" | wc -l)
- Products: $(tail -n +2 "$SOURCE_DIR/product_master.csv" | wc -l)
- Sales Transactions: $(tail -n +2 "$SOURCE_DIR"/sales_transactions_*.csv | wc -l)

Validation Status: PASSED
EOF

log_message "Pre-processing summary created: $SUMMARY_FILE"
log_message "=== Pre-processing completed successfully ==="

exit 0
