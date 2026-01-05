-- Table1 columns
SELECT name AS col, 'preprocessed.gbif_expert' AS table_name
FROM pragma_table_info('preprocessed.gbif_expert')
UNION ALL
-- Table2 columns
SELECT name AS col, 'preprocessed.citizen_obs_from_expert' AS table_name
FROM pragma_table_info('preprocessed.citizen_obs_from_expert')
ORDER BY col, table_name;
