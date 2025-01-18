INSERT INTO dgta_dataset
SELECT *
FROM file('/var/lib/clickhouse/user_files/data/dgta_dataset.json.gz', JSONEachRow);
