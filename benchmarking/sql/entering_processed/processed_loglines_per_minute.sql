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
ORDER BY time_bucket FORMAT CSVWithNames