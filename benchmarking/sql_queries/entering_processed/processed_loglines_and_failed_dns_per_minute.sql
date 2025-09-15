SELECT time_bucket,
    sum(count) AS total_count
FROM (
        SELECT toStartOfMinute(timestamp) AS time_bucket,
            count(DISTINCT logline_id) AS count
        FROM (
                SELECT logline_id,
                    timestamp
                FROM (
                        SELECT logline_id,
                            timestamp,
                            ROW_NUMBER() OVER (
                                PARTITION BY logline_id
                                ORDER BY timestamp DESC
                            ) AS rn
                        FROM logline_timestamps
                        WHERE is_active = false
                    )
                WHERE rn = 1
            )
        GROUP BY time_bucket
        UNION ALL
        SELECT toStartOfMinute(timestamp_failed) AS time_bucket,
            count(DISTINCT message_text) AS count
        FROM failed_dns_loglines
        GROUP BY time_bucket
    )
GROUP BY time_bucket
ORDER BY time_bucket FORMAT CSVWithNames
