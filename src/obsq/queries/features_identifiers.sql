CREATE OR REPLACE VIEW features.identifier AS


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
ROUND(AVG(id_time),2) AS obsv_avg_id_time
FROM id_time_diffs
GROUP BY "identifiedBy"
),

yearly_observation AS(

SELECT 
COUNT(*) AS yearly_observations,
"identifiedBy",
year
FROM distinct_ids
GROUP BY identifiedBy, year
),

monthly_observations AS(

SELECT 
COUNT(*) AS monthly_observations,
"identifiedBy",
year,
month
FROM distinct_ids
GROUP BY identifiedBy, year, month
),

species_count AS(

    SELECT "identifiedBy", species, COUNT(*) AS count
    FROM distinct_ids
    GROUP BY "identifiedBy", species
),

species_probability AS(
SELECT s."identifiedBy", s.species, s.count,
SUM(s.count) OVER (PARTITION BY s."identifiedBy") AS TotalCount,
CAST(s.count AS DECIMAL(10, 4)) / SUM(s.count) OVER (PARTITION BY s."identifiedBy") AS Probability
FROM species_count s
),

species_entropy AS(
SELECT p."identifiedBy",
ROUND(ABS(-SUM(p.Probability * LOG(2, p.Probability))),4)AS species_entropy -- LOG(2, value) for bASe-2 log
FROM species_probability p
GROUP BY p."identifiedBy"
)

SELECT d."identifiedBy",
c.id_count,
t.obsv_avg_id_time,
s.species_entropy AS id_species_entropy,
ROUND(AVG(m.monthly_observations),2) AS id_avg_monthly_obs,
ROUND(AVG(y.yearly_observations),2) AS id_avg_yearly_obs

FROM distinct_ids d
JOIN id_count c ON d."identifiedBy"= c.identifiedBy
JOIN id_time_stats t ON d."identifiedBy"= t.identifiedBy
JOIN species_entropy s ON d.identifiedBy = s."identifiedBy"
JOIN yearly_observation y ON d.identifiedBy = y."identifiedBy"
JOIN monthly_observations m ON d.identifiedBy = m."identifiedBy"

GROUP BY d.identifiedBy,
c.id_count,
t.obsv_avg_id_time,
s.species_entropy 
ORDER BY d.identifiedBy