CREATE OR REPLACE VIEW raw.all_observers AS 

SELECT DISTINCT
recordedBy,
recordedByID

FROM clean.gbif_citizen

