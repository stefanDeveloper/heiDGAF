import os
import sys

import clickhouse_connect

sys.path.append(os.getcwd())
from src.base.data_classes.clickhouse_connectors import TABLE_NAME_TO_TYPE


def get_tables():
    tables = {}

    for table_name in TABLE_NAME_TO_TYPE:
        tables[table_name] = []

    return tables


def query_once(client, tables):
    for table_name in tables.keys():
        tables[table_name] = client.query(f"SELECT * FROM {table_name} LIMIT 10;")

    return tables


def reset_tables(client, tables):
    for table_name in tables.keys():
        tables[table_name] = client.command(f"DROP TABLE {table_name};")


def main():
    client = clickhouse_connect.get_client(host="172.27.0.11", port=8123)
    tables = get_tables()

    results = query_once(client, tables)

    for key in results:
        print(f"'{key}':")

        if results[key].result_rows:
            for row in results[key].result_rows:
                print("\t", row)
        else:
            print("\t -")


if __name__ == "__main__":
    main()
