SELECT *
FROM fill_levels
WHERE stage = 'data_analysis.detector'
    AND entry_type = 'total_loglines'
ORDER BY timestamp ASC FORMAT CSVWithNames