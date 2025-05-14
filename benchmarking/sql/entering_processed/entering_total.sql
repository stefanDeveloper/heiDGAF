SELECT timestamp_in,
    count(DISTINCT message_id) OVER (
        ORDER BY timestamp_in
    ) AS cumulative_count
FROM (
        SELECT timestamp_in,
            message_id
        FROM server_logs
    ) FORMAT CSVWithNames