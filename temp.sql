CREATE OR REPLACE TABLE clean.temp AS 

WITH cast_json AS(
SELECT "gbifID",
CAST(g.dynamicProperties AS JSON) as annotations

FROM raw.gbif_citizen g
WHERE institutionCode = 'iNaturalist'

),

json_extracted AS(

SELECT t."gbifID", e.key, e.value
FROM cast_json t,
     json_each(try_cast(t.annotations AS JSON)) e
),

json_first_only AS(

    SELECT gbifID,
CASE
  WHEN NOT json_valid(value) THEN NULL
  WHEN json_type(value) = 'ARRAY'
       THEN json_extract(value, '$[0]')
  ELSE value
END AS value
FROM json_extracted
)

SELECT gbifID,
value,
CASE replace(value, '"', '')
    WHEN 'no live leaves' THEN 40
    WHEN 'green leaves' THEN 38
    WHEN 'breaking leaf buds' THEN 37
    WHEN 'colored leaves' THEN 39
    ELSE -1
END AS pheno_leaves

FROM json_first_only 
