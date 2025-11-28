CREATE OR REPLACE VIEW raw.citizen_observers AS
SELECT recordedBy
FROM raw.all_observers
WHERE recordedByID is NULL
