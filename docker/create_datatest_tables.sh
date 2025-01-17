#!/bin/bash

echo "Creating tables for normal operation..."

for script in /create_tables/*.sql; do
  echo "Executing $script..."
  clickhouse-client --host=127.0.0.1 --query="$(cat $script)"
done

echo "Initializing datatest tables..."

for script in /create_datatest_tables/*.sql; do
  echo "Executing $script..."
  clickhouse-client --host=127.0.0.1 --query="$(cat $script)"
done

echo "Initialization complete!"
