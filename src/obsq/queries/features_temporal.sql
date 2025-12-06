CREATE OR REPLACE VIEW features.temporal AS
SELECT gbifID,
day,
month,
year,
eventDate
-- probability density from histogram 

FROM labeled.gbif_citizen