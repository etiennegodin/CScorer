CREATE OR REPLACE VIEW observers.citizen AS
SELECT recordedBy
FROM raw.all_observers
WHERE recordedByID is NULL
