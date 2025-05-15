SELECT sbt2.timestamp AS time,
    dateDiff(microsecond, sbt1.timestamp, sbt2.timestamp) AS value
FROM suspicious_batch_timestamps sbt1
    INNER JOIN suspicious_batch_timestamps sbt2 ON sbt1.suspicious_batch_id = sbt2.suspicious_batch_id
WHERE sbt1.stage = 'data_analysis.detector'
    AND sbt1.status = 'in_process'
    AND sbt2.stage = 'data_analysis.detector'
    AND sbt2.is_active = False
ORDER BY time ASC FORMAT CSVWithNames