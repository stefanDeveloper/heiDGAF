import clickhouse_connect

QUERY_TABLE = "dns_loglines"

client = clickhouse_connect.get_client(host="172.27.0.11", port=8123)

result = client.query(f"SELECT * FROM {QUERY_TABLE};")

for row in result.result_rows:
    print(row)
