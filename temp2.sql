CREATE OR REPLACE VIEW labeled.gbif_citizen_cleaned AS
SELECT *,
CAST(annotations AS JSON) as clean

FROM labeled.gbif_citizen
;

SELECT t."gbifID", e.key, e.value
FROM labeled.gbif_citizen t,
     json_each(try_cast(t.annotations AS JSON)) e;