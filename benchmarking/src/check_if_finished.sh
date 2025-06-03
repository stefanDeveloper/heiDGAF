#!/bin/bash
set -euo pipefail

# Configuration
CONTAINER="clickhouse-server"
CLICKHOUSE_CLIENT="clickhouse-client"
SQL_DIR="sql"
CHECK_INTERVAL=30  # seconds

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to run query from file
run_query_file() {
    local input_file="$1"
    sudo docker exec -i "$CONTAINER" $CLICKHOUSE_CLIENT --query "$(cat "$input_file")" --format CSV 2>/dev/null
}

#------------ Check if test has terminated -----------
check_count=0

while true; do
    ((check_count++))
    log_message "Check #$check_count"

    file="$SQL_DIR/entering_processed/activity_last_three_minutes.sql"
    result=$(run_query_file "$file")
    query_exit_code=$?

    if [ $query_exit_code -ne 0 ]; then
        log_message "WARNING: Query execution failed (exit code: $query_exit_code)"
        sleep "$CHECK_INTERVAL"
        continue
    fi

    if [ "$result" -eq 0 ]; then
        log_message "SUCCESS: Test terminated – no recent activity (sum of last 3 = 0)"
        echo "TEST_TERMINATED"
        exit 0
    else
        log_message "Test still running – recent activity detected (count = $result)"
    fi

    sleep "$CHECK_INTERVAL"
done

log_message "TIMEOUT: Maximum number of checks reached without test termination"
echo "TEST_TIMEOUT"
exit 3
