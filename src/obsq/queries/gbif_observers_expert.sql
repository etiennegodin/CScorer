CREATE OR REPLACE VIEW observers.expert AS
SELECT *
FROM raw.all_observers
WHERE recordedByID is NOT NULL OR recordedBy IN ('Jacques Ranger')
