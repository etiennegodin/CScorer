CREATE OR REPLACE VIEW observers.citizen AS
SELECT *
FROM observers.all
WHERE recordedByID is NULL
