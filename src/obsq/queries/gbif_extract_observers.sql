CREATE OR REPLACE VIEW raw.all_observers AS 

SELECT 
recordedBy,
recordedByID

FROM clean.gbif_citizen

