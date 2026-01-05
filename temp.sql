CREATE OR REPLACE TABLE clean.temp AS 

WITH cast_json AS(
SELECT "gbifID",
CAST(g.dynamicProperties AS JSON) as annotations

FROM raw.gbif_citizen g
AND institutionCode = 'iNaturalist'

)

SELECT *
FROM cast_json