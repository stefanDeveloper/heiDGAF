#!/bin/bash
set -euo pipefail

# Configuration
CONTAINER="clickhouse-server"
CLICKHOUSE_CLIENT="clickhouse-client"

declare -a TABLES=("alerts" "batch_timestamps" "dns_loglines" "failed_dns_loglines" "fill_levels" "logline_timestamps" "logline_to_batches" "server_logs" "server_logs_timestamps" "suspicious_batch_timestamps" "suspicious_batches_to_batch")

# Function to drop single table
drop_table() {
    local table_name="$1"
    echo "Dropping table: $table_name"
    docker exec -i "$CONTAINER" $CLICKHOUSE_CLIENT --query "TRUNCATE TABLE $table_name"
}

# ---------- Drop all tables ----------
for table in "${TABLES[@]}"; do
    drop_table "$table"
done

echo "✅   Clean up successful."
