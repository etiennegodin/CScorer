CREATE OR REPLACE VIEW observers.all AS 

SELECT DISTINCT
recordedBy,
recordedByID

FROM clean.gbif_citizen

