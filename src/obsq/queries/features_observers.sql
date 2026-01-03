
-- obs count per species per obs
CREATE OR REPLACE VIEW preprocessed.observer_species_stats AS (

WITH species_count AS(

SELECT
COUNT(*) as species_obs,
"recordedBy", "species"
FROM labeled.gbif_citizen
GROUP BY "recordedBy", species
)

SELECT
recordedBy,
ROUND(AVG(species_obs),2) as avg_species_obs_count,
MAX(species_obs) as max_species_obs_count

FROM species_count
GROUP BY "recordedBy"
);

-- main features
CREATE OR REPLACE TABLE features.observer AS

WITH inat_data AS (

SELECT 
    id AS inat_id,
    name AS inat_name,
    roles,
    login_autocomplete as inat_login,
    observations_count AS inat_obs_count,
    identifications_count AS inat_id_count,
    species_count AS inat_species_count,
    universal_search_rank AS inat_total

FROM observers.inat_data

),

yearly_observation AS(

SELECT 
COUNT(*) as yearly_observations,
"recordedBy",
year
FROM labeled.gbif_citizen
GROUP BY recordedBy, year
),

monthly_observations AS(

SELECT 
COUNT(*) as monthly_observations,
"recordedBy",
year,
month
FROM labeled.gbif_citizen
GROUP BY recordedBy, year, month
) 

SELECT g.recordedBy,
--counts
COUNT(DISTINCT g.gbifID) as obsv_obs_count, 
ROUND(COUNT(DISTINCT g.gbifID) / SUM(COUNT(DISTINCT "gbifID")) OVER (), 5) AS obsv_total_pct,
-- ids
COUNT(DISTINCT g.gbifID) FILTER (WHERE g.identifiedByID IS NOT NULL) AS obsv_expert_ids,
ROUND(obsv_expert_ids / obsv_obs_count, 3) as obsv_expert_ids_pct,
COUNT( DISTINCT expert_match) FILTER ( WHERE g.expert_match = 1 )AS obsv_expert_match_count,
ROUND(obsv_expert_match_count / obsv_obs_count,4) as obsv_expert_match_pct,
ROUND(AVG(CAST("dateIdentified" AS DATE) - CAST("eventDate" AS DATE))) as obsv_avg_id_time,
--taxonomic
COUNT(DISTINCT g.class) as obsv_class_count,
COUNT(DISTINCT g."order") as obsv_order_count,
COUNT(DISTINCT g.family) as obsv_family_count,
COUNT(DISTINCT g.genus) as obsv_genus_count,
COUNT(DISTINCT g.species) as obsv_species_count,
-- time
COUNT(DISTINCT g."year") as obsv_unique_year_count, 
COUNT(DISTINCT CAST (g.eventDate AS DATE)) as obsv_unique_dates,
MAX(y.yearly_observations) as obsv_max_yearly_obs,
MAX(m.monthly_observations) as obsv_max_monthly_obs,
ROUND(AVG(y.yearly_observations),2) as obsv_avg_yearly_obs,
ROUND(AVG(m.monthly_observations),2) as obsv_avg_monthly_obs,
--
COUNT(DISTINCT g.coordinateUncertaintyInMeters ) FILTER (WHERE g.coordinateUncertaintyInMeters > 1000 ) as obsv_high_cood_un_obs, -- count obs with high uncer
ROUND(obsv_high_cood_un_obs / obsv_obs_count, 3) as obsv_high_cood_un_pct,
ROUND(AVG(g.coordinateUncertaintyInMeters),3) as obsv_avg_coord_un,
ROUND(AVG(g.media_count),2) as obsv_avg_media_count,
ROUND(COUNT(DISTINCT g.sex ) FILTER ( WHERE g.sex IS NOT NULL )/obsv_obs_count, 3) AS obsv_sex_meta_pct,
ROUND(COUNT(DISTINCT g.reproductiveCondition) FILTER ( WHERE g.reproductiveCondition IS NOT NULL )/obsv_obs_count, 3) AS obsv_repro_cond_meta_pct,
ROUND(COUNT(DISTINCT g.annotations) FILTER ( WHERE g.annotations IS NOT NULL )/obsv_obs_count, 3) AS obsv_annot_meta_pct,
CASE
    WHEN ROUND(AVG(LENGTH("occurrenceRemarks"))) IS NULL THEN 0
    ELSE ROUND(AVG(LENGTH("occurrenceRemarks"))) 
END AS obsv_avg_descr_len


FROM labeled.gbif_citizen g
JOIN yearly_observation y
    ON g.recordedBy = y.recordedBy
JOIN monthly_observations m 
    ON g.recordedBy = m.recordedBy
GROUP BY g.recordedBy
;

--adding back species obs count
ALTER TABLE features.observer ADD COLUMN IF NOT EXISTS obsv_avg_species_obs_count FLOAT;
ALTER TABLE features.observer ADD COLUMN IF NOT EXISTS obsv_max_species_obs_count INT;
UPDATE features.observer o
SET obsv_avg_species_obs_count = s.avg_species_obs_count,
    obsv_max_species_obs_count = s.max_species_obs_count
FROM preprocessed.observer_species_stats s
WHERE o.recordedBy = s.recordedBy;
