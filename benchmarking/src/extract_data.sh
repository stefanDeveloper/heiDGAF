#!/bin/bash
set -euo pipefail

# Configuration
CONTAINER="fafd2d59cf6e"
CLICKHOUSE_CLIENT="clickhouse-client"
BASE_DIR="../benchmark_results/test_run"
SQL_DIR="sql"

# Create output directories
declare -a SUBDIRS=("entering_processed" "latencies" "log_volumes" "full_tables")
for dir in "${SUBDIRS[@]}"; do
    mkdir -p "$BASE_DIR/$dir"
done
echo "✅ Created output folders under $BASE_DIR"

# Function to run query from file
run_query_file() {
    local input_file="$1"
    local output_file="$2"
    echo "🔹 Running: $input_file -> $output_file"
    sudo docker exec -i "$CONTAINER" $CLICKHOUSE_CLIENT --query "$(cat "$input_file")" > "$output_file"
}

# ---------- Process all query files ----------
for subdir in "${SUBDIRS[@]}"; do
    for sql_file in "$SQL_DIR/$subdir"/*.sql; do
        filename=$(basename "$sql_file" .sql)
        output_path="$BASE_DIR/$subdir/${filename}.csv"
        run_query_file "$sql_file" "$output_path"
    done
done

echo "✅ All SQL scripts executed successfully."
