
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
COUNT(DISTINCT g.gbifID) as observations_count, 
ROUND(COUNT(DISTINCT g.gbifID) / SUM(COUNT(DISTINCT "gbifID")) OVER (), 5) AS total_pct,
-- ids
COUNT(DISTINCT g.gbifID) FILTER (WHERE g.identifiedByID IS NOT NULL) AS expert_ids,
ROUND(expert_ids / observations_count, 3) as expert_ids_pct,
COUNT( DISTINCT expert_match) FILTER ( WHERE g.expert_match = 1 )AS expert_match_count,
ROUND(expert_match_count / observations_count,4) as expert_match_pct,
ROUND(AVG(CAST("dateIdentified" AS DATE) - CAST("eventDate" AS DATE))) as avg_id_time,
--taxonomic
COUNT(DISTINCT g.class) as class_count,
COUNT(DISTINCT g."order") as order_count,
COUNT(DISTINCT g.family) as family_count,
COUNT(DISTINCT g.genus) as genus_count,
COUNT(DISTINCT g.species) as species_count,
-- time
COUNT(DISTINCT g."year") as unique_year_count, 
COUNT(DISTINCT CAST (g.eventDate AS DATE)) as unique_dates,
MAX(y.yearly_observations) as max_yearly_observations,
MAX(m.monthly_observations) as max_monthly_observations,
ROUND(AVG(y.yearly_observations),2) as avg_yearly_observations,
ROUND(AVG(m.monthly_observations),2) as avg_monthly_observations,
--
COUNT(DISTINCT g.coordinateUncertaintyInMeters ) FILTER (WHERE g.coordinateUncertaintyInMeters > 1000 ) as high_cood_un_obs, -- count obs with high uncer
ROUND(high_cood_un_obs / observations_count, 3) as high_cood_un_pct,
ROUND(AVG(g.coordinateUncertaintyInMeters),3) as avg_coord_un,
ROUND(AVG(g.media_count),2) as avg_media_count,
ROUND(COUNT(DISTINCT g.sex ) FILTER ( WHERE g.sex IS NOT NULL )/observations_count, 3) AS sex_meta_pct,
ROUND(COUNT(DISTINCT g.reproductiveCondition) FILTER ( WHERE g.reproductiveCondition IS NOT NULL )/observations_count, 3) AS reproductiveCondition_meta_pct,
ROUND(COUNT(DISTINCT g.annotations) FILTER ( WHERE g.annotations IS NOT NULL )/observations_count, 3) AS annotations_meta_pct,
ROUND(AVG(LENGTH("occurrenceRemarks"))) AS avg_description_len


FROM labeled.gbif_citizen g
JOIN yearly_observation y
    ON g.recordedBy = y.recordedBy
JOIN monthly_observations m 
    ON g.recordedBy = m.recordedBy
GROUP BY g.recordedBy;

--adding back species obs count
ALTER TABLE features.observer ADD COLUMN avg_species_obs_count FLOAT;
ALTER TABLE features.observer ADD COLUMN max_species_obs_count INT;
UPDATE features.observer o
SET avg_species_obs_count = s.avg_species_obs_count,
    max_species_obs_count = s.max_species_obs_count
FROM preprocessed.observer_species_stats s
WHERE o.recordedBy = s.recordedBy;
