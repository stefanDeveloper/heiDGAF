SELECT *
FROM fill_levels
WHERE stage = 'data_inspection.inspector'
    AND entry_type = 'total_loglines'
ORDER BY timestamp ASC FORMAT CSVWithNames