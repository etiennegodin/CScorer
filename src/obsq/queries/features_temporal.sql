CREATE OR REPLACE VIEW features.temporal AS
SELECT gbifID,
day AS tempo_day,
month AS tempo_month,
year AS tempo_year,
--eventDate AS tempo_eventDate
-- probability density from histogram 

FROM labeled.gbif_citizen