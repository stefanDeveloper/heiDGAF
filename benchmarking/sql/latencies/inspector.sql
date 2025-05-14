SELECT bt2.timestamp AS time,
    dateDiff(microsecond, bt1.timestamp, bt2.timestamp) AS value
FROM batch_timestamps bt1
    INNER JOIN batch_timestamps bt2 ON bt1.batch_id = bt2.batch_id
WHERE bt1.stage = 'data_inspection.inspector'
    AND bt1.status = 'in_process'
    AND bt2.stage = 'data_inspection.inspector'
    AND bt2.is_active = False
ORDER BY time ASC
UNION ALL
SELECT sbt.timestamp AS time,
    dateDiff(microsecond, bt.timestamp, sbt.timestamp) AS value
FROM batch_timestamps bt
    INNER JOIN suspicious_batches_to_batch sbtb ON bt.batch_id = sbtb.batch_id
    INNER JOIN suspicious_batch_timestamps sbt ON sbtb.suspicious_batch_id = sbt.suspicious_batch_id
WHERE bt.stage = 'data_inspection.inspector'
    AND bt.status = 'in_process'
    AND sbt.stage = 'data_inspection.inspector'
    AND sbt.status = 'finished'
ORDER BY time ASC FORMAT CSVWithNames