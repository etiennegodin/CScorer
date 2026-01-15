CREATE OR REPLACE VIEW features.identifiers AS


WITH distinct_ids AS(

SELECT * FROM preprocessed.gbif_citizen WHERE "identifiedBy" != "recordedBy"
),
-- id counts 
id_count AS(

SELECT "identifiedBy",
COUNT(*)  AS id_count,
FROM distinct_ids
GROUP BY "identifiedBy"
ORDER BY "identifiedBy"ASC
),

id_time_diffs AS(
    SELECT
    "identifiedBy",
    ABS(CAST(datediff('day', CAST(dateIdentified AS DATE), CAST(eventDate AS DATE)) AS INT )) AS id_time
    FROM distinct_ids

), id_time_stats AS(

SELECT "identifiedBy",
AVG(id_time) AS obsv_avg_id_time
FROM id_time_diffs
GROUP BY "identifiedBy"
)

SELECT d."identifiedBy",
c.id_count,
t.obsv_avg_id_time




FROM distinct_ids d
JOIN id_count c ON d."identifiedBy"= c.identifiedBy
JOIN id_time_stats t ON d."identifiedBy"= t.identifiedBy