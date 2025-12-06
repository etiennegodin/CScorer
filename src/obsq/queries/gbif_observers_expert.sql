CREATE OR REPLACE VIEW observers.expert AS
SELECT *
FROM observers.all
WHERE recordedByID is NOT NULL OR recordedBy IN ('Jacques Ranger')
