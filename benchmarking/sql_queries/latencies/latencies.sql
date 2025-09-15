SELECT slt.event_timestamp AS time,
    dateDiff(
        microsecond,
        sl.timestamp_in,
        slt.event_timestamp
    ) AS value
FROM server_logs sl
    INNER JOIN server_logs_timestamps slt ON sl.message_id = slt.message_id
WHERE slt.event = 'timestamp_out'
ORDER BY time ASC FORMAT CSVWithNames
