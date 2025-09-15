SELECT timestamp,
    entry_count
FROM fill_levels
WHERE stage = 'log_collection.batch_handler'
    AND entry_type = 'total_loglines_in_buffer'
ORDER BY timestamp ASC FORMAT CSVWithNames
