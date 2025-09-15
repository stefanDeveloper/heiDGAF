SELECT toStartOfMinute(toDateTime64(timestamp_in, 6)) AS time_bucket,
    count(*)
FROM server_logs
GROUP BY time_bucket
ORDER BY time_bucket FORMAT CSVWithNames
