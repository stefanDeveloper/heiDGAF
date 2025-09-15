SELECT *
FROM fill_levels
WHERE stage = 'log_collection.collector'
    AND entry_type = 'total_loglines'
ORDER BY timestamp ASC FORMAT CSVWithNames
