SELECT sum(count) AS total_count
FROM (
    SELECT count(*) AS count
    FROM (
        SELECT logline_id
        FROM (
            SELECT
                logline_id,
                timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY logline_id
                    ORDER BY timestamp DESC
                ) AS rn
            FROM logline_timestamps
            WHERE is_active = false
              AND timestamp >= now() - INTERVAL 3 MINUTE
        )
        WHERE rn = 1
    )

    UNION ALL

    SELECT count(*) AS count
    FROM failed_dns_loglines
    WHERE timestamp_failed >= now() - INTERVAL 3 MINUTE

    UNION ALL

    SELECT count(*) AS count
    FROM server_logs
    WHERE timestamp_in >= now() - INTERVAL 3 MINUTE
)
