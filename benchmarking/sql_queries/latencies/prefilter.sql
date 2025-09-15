SELECT bt2.timestamp AS time,
    dateDiff(microsecond, bt1.timestamp, bt2.timestamp) AS value
FROM batch_timestamps bt1
    INNER JOIN batch_timestamps bt2 ON bt1.batch_id = bt2.batch_id
WHERE bt1.status = 'in_process'
    AND bt2.status = 'finished'
    AND bt1.stage = 'log_filtering.prefilter'
    AND bt2.stage = 'log_filtering.prefilter'
ORDER BY time ASC FORMAT CSVWithNames
