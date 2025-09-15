SELECT lt2.timestamp AS time,
    dateDiff(microsecond, lt1.timestamp, lt2.timestamp) AS value
FROM logline_timestamps lt1
    INNER JOIN logline_timestamps lt2 ON lt1.logline_id = lt2.logline_id
WHERE lt1.stage = 'log_collection.batch_handler'
    AND lt1.status = 'in_process'
    AND lt2.stage = 'log_collection.batch_handler'
    AND lt2.status = 'batched'
ORDER BY time ASC FORMAT CSVWithNames
