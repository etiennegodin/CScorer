CREATE OR REPLACE TABLE features.observer AS

WITH yearly_observation AS(

SELECT 
COUNT(*) AS yearly_observations,
"recordedBy",
year
FROM preprocessed.gbif_citizen
GROUP BY recordedBy, year
),

obsv_count AS(

    SELECT "recordedBy",
    COUNT(DISTINCT g.gbifID) AS obsv_obs_count, 
    FROM preprocessed.gbif_citizen g
    GROUP BY g.recordedBy,

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

--time
COUNT(DISTINCT g."year") AS obsv_unique_year_count, 
COUNT(DISTINCT CAST (g.eventDate AS DATE)) AS obsv_unique_dates,
obsv_obs_count / obsv_unique_year_count AS obs_per_year,
ROUND(AVG(y.yearly_observations),2) AS obsv_avg_yearly_obs,

FROM preprocessed.gbif_citizen g
JOIN yearly_observation y ON g.recordedBy = y.recordedBy
JOIN species_entropy p ON g."recordedBy" = p."recordedBy"


GROUP BY g.recordedBy,
p.species_entropy
ORDER BY g.recordedBy


;


ALTER TABLE features.observer ADD COLUMN IF NOT EXISTS obv_log_z_score FLOAT;
ALTER TABLE features.observer ADD COLUMN IF NOT EXISTS sp_log_z_score FLOAT;
ALTER TABLE features.observer ADD COLUMN IF NOT EXISTS obs_p_rank FLOAT;
ALTER TABLE features.observer ADD COLUMN IF NOT EXISTS sp_p_rank FLOAT;
ALTER TABLE features.observer ADD COLUMN IF NOT EXISTS experience FLOAT;


WITH obsv_count AS(

    SELECT "recordedBy",
    COUNT(DISTINCT g.gbifID) AS obsv_obs_count, 
    FROM preprocessed.gbif_citizen g
    GROUP BY g.recordedBy,

),

species_count AS(

    SELECT "recordedBy", species, COUNT(*) AS count
    FROM preprocessed.gbif_citizen
    GROUP BY "recordedBy", species
),

pre_stats AS (
    SELECT
        AVG(LOG(1 + o.obsv_obs_count)) AS obv_mean_log,
        STDDEV_POP(LOG(1 + o.obsv_obs_count)) AS obv_std_log,

        AVG(LOG(1 + s.count)) AS sp_mean_log,
        STDDEV_POP(LOG(1 + count)) AS sp_std_log,

    FROM obsv_count o 
    JOIN species_count s ON o."recordedBy" = s."recordedBy"
),

stats AS(

SELECT
    o."recordedBy",
    LOG(1 + o.obsv_obs_count) / 10 as experience,
    (LOG(1 + o.obsv_obs_count) - p.obv_mean_log)
        / NULLIF(p.obv_std_log, 0) AS obv_log_z_score,

    (LOG(1 + s.count) - p.sp_mean_log)
        / NULLIF(p.sp_std_log, 0) AS sp_log_z_score,

    ROUND(PERCENT_RANK() OVER (ORDER BY o.obsv_obs_count),3) AS obs_p_rank,
    ROUND(PERCENT_RANK() OVER (ORDER BY s.count),3) AS sp_p_rank,

    FROM obsv_count o
    JOIN species_count s ON o."recordedBy" = s."recordedBy"
    CROSS JOIN pre_stats p
)

UPDATE features.observer o
SET obv_log_z_score = s.obv_log_z_score,
sp_log_z_score = s.sp_log_z_score,
obs_p_rank = s.obs_p_rank,
sp_p_rank = s.sp_p_rank,
experience = s.experience

FROM stats s
WHERE o."recordedBy" = s."recordedBy";


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
