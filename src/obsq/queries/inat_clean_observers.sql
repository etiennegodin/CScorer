
-- create table with name matches
CREATE OR REPLACE VIEW observers.inat_data AS

SELECT *
FROM raw.inat_observers_extracted
WHERE universal_search_rank > 1;

