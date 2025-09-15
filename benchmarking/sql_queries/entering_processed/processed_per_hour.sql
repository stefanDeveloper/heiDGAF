WITH per_second AS (
    SELECT toStartOfSecond(toDateTime64(timestamp_failed, 6)) AS time_bucket,
        count(DISTINCT message_text) AS number
    FROM failed_dns_loglines
    GROUP BY time_bucket
    UNION ALL
    SELECT toStartOfSecond(toDateTime64(timestamp, 6)) AS time_bucket,
        count(DISTINCT logline_id) AS number
    FROM logline_timestamps
    WHERE is_active = False
    GROUP BY time_bucket
),
per_minute AS (
    SELECT toStartOfMinute(time_bucket) AS time_bucket,
        sum(number) AS number
    FROM per_second
    GROUP BY time_bucket
),
per_hour AS (
    SELECT toStartOfHour(time_bucket) AS time_bucket,
        sum(number) AS number
    FROM per_minute
    GROUP BY time_bucket
)
SELECT time_bucket,
    sum(number) AS count
FROM per_hour
GROUP BY time_bucket FORMAT CSVWithNames
