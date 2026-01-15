CREATE OR REPLACE TABLE features.observer AS

WITH yearly_observation AS(

SELECT 
COUNT(*) AS yearly_observations,
"recordedBy",
year
FROM preprocessed.gbif_citizen
GROUP BY recordedBy, year
),

monthly_observations AS(

SELECT 
COUNT(*) AS monthly_observations,
"recordedBy",
year,
month
FROM preprocessed.gbif_citizen
GROUP BY recordedBy, year, month
),

species_count AS(

    SELECT "recordedBy", species, COUNT(*) AS count
    FROM preprocessed.gbif_citizen
    GROUP BY "recordedBy", species
),

species_probability AS(
SELECT s."recordedBy", s.species, s.count,
SUM(s.count) OVER (PARTITION BY s."recordedBy") AS TotalCount,
CAST(s.count AS DECIMAL(10, 4)) / SUM(s.count) OVER (PARTITION BY s."recordedBy") AS Probability
FROM species_count s
),

species_entropy AS(
SELECT p."recordedBy",
ROUND(ABS(-SUM(p.Probability * LOG(2, p.Probability))),4)AS species_entropy -- LOG(2, value) for bASe-2 log
FROM species_probability p
GROUP BY p."recordedBy"
)

SELECT g.recordedBy,

--taxonomic
p.species_entropy AS obsv_species_diversity,
--counts
COUNT(DISTINCT g.gbifID) AS obsv_obs_count, 
-- ids
ROUND(COUNT(DISTINCT g.gbifID) FILTER (WHERE g.identifiedByID IS NOT NULL) /
 obsv_obs_count, 3) AS obsv_expert_ids_pct,
--time
COUNT(DISTINCT g."year") AS obsv_unique_year_count, 
COUNT(DISTINCT CAST (g.eventDate AS DATE)) AS obsv_unique_dates,
obsv_obs_count / obsv_unique_year_count AS obs_per_year,
ROUND(AVG(m.monthly_observations),2) AS obsv_avg_monthly_obs,
ROUND(AVG(y.yearly_observations),2) AS obsv_avg_yearly_obs,

-- pct obs with high uncer
ROUND(COUNT(DISTINCT g.coordinateUncertaintyInMeters ) FILTER (WHERE g.coordinateUncertaintyInMeters > 1000 ) /
 obsv_obs_count, 3) AS obsv_high_cood_un_pct,

ROUND(AVG(g.coordinateUncertaintyInMeters),3) AS obsv_avg_coord_un,
ROUND(AVG(g.media_count),2) AS obsv_avg_media_count,
ROUND(COUNT(DISTINCT g.pheno_sex ) FILTER ( WHERE g.pheno_sex IS NOT NULL )/obsv_obs_count, 3) AS obsv_sex_meta_pct,
ROUND(COUNT(DISTINCT g.pheno_repro) FILTER ( WHERE g.pheno_repro IS NOT NULL )/obsv_obs_count, 3) AS obsv_repro_cond_meta_pct,
ROUND(COUNT(DISTINCT g.pheno_leaves) FILTER ( WHERE g.pheno_leaves IS NOT NULL )/obsv_obs_count, 3) AS obsv_annot_meta_pct,
CASE
    WHEN ROUND(AVG(LENGTH("occurrenceRemarks"))) IS NULL THEN 0
    ELSE ROUND(AVG(LENGTH("occurrenceRemarks"))) 
END AS obsv_avg_descr_len

FROM preprocessed.gbif_citizen g
JOIN yearly_observation y ON g.recordedBy = y.recordedBy
JOIN monthly_observations m ON g.recordedBy = m.recordedBy
JOIN species_entropy p ON g."recordedBy" = p."recordedBy"

GROUP BY g.recordedBy,
p.species_entropy
ORDER BY g.recordedBy
;


-- id counts 
ALTER TABLE features.observer ADD COLUMN IF NOT EXISTS obsv_avg_obs_time FLOAT;

id_count AS(


SELECT g.identifiedBy,
COUNT(g.identifiedBy) FILTER (WHERE g.identifiedBy != g.recordedBy) AS obsv_id_count,
FROM preprocessed.gbif_citizen g 
GROUP BY g.identifiedBy
ORDER BY g.identifiedBy ASC
)


-- time between observations
ALTER TABLE features.observer ADD COLUMN IF NOT EXISTS obsv_avg_obs_time FLOAT;
WITH obs_time_diffs AS(
    SELECT
    "recordedBy",
    CAST(eventDate AS DATE)AS eventDate ,
    CAST(LAG(eventDate, 1) OVER (PARTITION BY "recordedBy" ORDER BY eventDate) AS DATE) AS PriorDate,
    CAST(datediff('day', PriorDate, CAST(eventDate AS DATE)) AS INT) AS DaysBetweenEvents,
    FROM preprocessed.gbif_citizen

), obs_time_stats AS(

SELECT "recordedBy",
AVG(DaysBetweenEvents) as obsv_avg_obs_time, -- -1 if only one obs 
FROM obs_time_diffs
GROUP BY "recordedBy"
)

UPDATE features.observer o
SET obsv_avg_obs_time = t.obsv_avg_obs_time
FROM obs_time_stats t
WHERE o."recordedBy" = t."recordedBy";


--id time from users 
ALTER TABLE features.observer ADD COLUMN IF NOT EXISTS obsv_avg_id_time FLOAT;

WITH distinct_ids AS(

SELECT * FROM preprocessed.gbif_citizen WHERE "identifiedBy" != "recordedBy"
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

UPDATE features.observer o
SET obsv_avg_id_time = t.obsv_avg_id_time,
FROM id_time_stats t
WHERE o."recordedBy" = t."identifiedBy";


#ALTER TABLE features.observer DROP COLUMN IF EXISTS obsv_obs_count;
