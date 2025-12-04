CREATE OR REPLACE VIEW observers.citizen AS
SELECT recordedBy
FROM observers.all
WHERE recordedByID is NULL
