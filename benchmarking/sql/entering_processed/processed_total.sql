SELECT timestamp,
    count(DISTINCT value) OVER (
        ORDER BY timestamp
    ) AS cumulative_count
FROM (
        SELECT timestamp_failed AS timestamp,
            message_text AS value
        FROM failed_dns_loglines
        UNION ALL
        SELECT timestamp,
            toString(logline_id) AS value
        FROM logline_timestamps
        WHERE is_active = False
    ) FORMAT CSVWithNames