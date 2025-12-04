CREATE OR REPLACE VIEW observers.all AS 

SELECT DISTINCT
recordedBy,
recordedByID,
user_login,
user_id

FROM preprocessed.gbif_citizen_inat

