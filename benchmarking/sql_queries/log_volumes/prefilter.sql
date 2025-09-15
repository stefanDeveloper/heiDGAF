SELECT *
FROM fill_levels
WHERE stage = 'log_filtering.prefilter'
    AND entry_type = 'total_loglines'
ORDER BY timestamp ASC FORMAT CSVWithNames
