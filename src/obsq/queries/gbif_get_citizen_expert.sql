CREATE OR REPLACE VIEW raw.citizen_expert AS
SELECT *
FROM raw.all_observers
WHERE recordedByID is NOT NULL OR recordedBy IN ('Jacques Ranger')
