SELECT lt2.timestamp as time,
    dateDiff(microsecond, lt1.timestamp, lt2.timestamp) as value
FROM logline_timestamps lt1
    INNER JOIN logline_timestamps lt2 ON lt1.logline_id = lt2.logline_id
WHERE lt1.status = 'in_process'
    AND lt2.status = 'finished'
    AND lt1.stage = 'log_collection.collector'
    AND lt2.stage = 'log_collection.collector'
ORDER BY time ASC FORMAT CSVWithNames