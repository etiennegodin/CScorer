
-- obs count per species per obs
CREATE OR REPLACE VIEW features.observer_species_obs AS (

WITH species_count AS(

SELECT
COUNT(*) as species_obs,
"recordedBy", species
FROM preprocessed.gbif_citizen_labeled
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
    "json" ->> 'id' AS inat_id,
    "json" ->> 'name' AS inat_name,
    "json" ->> 'orcid' AS orcid,
    "json" ->> 'observations_count' AS inat_obs_count,
    "json" ->> 'identifications_count' AS id_count,
    "json" ->> 'species_count' AS inat_species_count,


FROM raw.inat_observers

),

yearly_observation AS(

SELECT 
COUNT(*) as yearly_observations,
"recordedBy",
year
FROM preprocessed.gbif_citizen_labeled
GROUP BY recordedBy, year

),

monthly_observations AS(

SELECT 
COUNT(*) as monthly_observations,
"recordedBy",
year,
month
FROM preprocessed.gbif_citizen_labeled
GROUP BY recordedBy, year,month
)

SELECT g.recordedBy,
COUNT(*) as observations_count,
ROUND(100.0000 * COUNT(*) / SUM(COUNT(*)) OVER (), 3) AS total_pct,
COUNT(DISTINCT class) as class_count,
COUNT(DISTINCT "order") as order_count,
COUNT(DISTINCT family) as family_count,
COUNT(DISTINCT g.genus) as genus_count,
COUNT(DISTINCT g.species) as species_count,
COUNT(DISTINCT g."month") as unique_month_count, 
COUNT(DISTINCT g."year") as unique_year_count, 
COUNT(DISTINCT cast(eventDate AS DATE)) as unique_dates,
MAX(y.yearly_observations) as max_yearly_observations,
MAX(m.monthly_observations) as max_monthly_observations,
ROUND(AVG(y.yearly_observations),2) as avg_yearly_observations,
ROUND(AVG(m.monthly_observations),2) as avg_monthly_observations,
COUNT(*) FILTER (WHERE "coordinateUncertaintyInMeters" > 1000 ) as high_cood_un_obs, -- count obs with high uncer
ROUND(high_cood_un_obs / observations_count, 2) as high_cood_un_pct,
ROUND(AVG(coordinateUncertaintyInMeters),2) as avg_coord_un,
MAX(coordinateUncertaintyInMeters) as max_coord_un,
ROUND(AVG(media_count),2) as avg_media_count,
SUM(num_identification_agreements) as id_agree_count,
ROUND(SUM(num_identification_agreements) / COUNT(num_identification_agreements),2)as id_agree_pct,
SUM(num_identification_disagreements) as id_disagree_count,
ROUND((SUM(num_identification_disagreements) / COUNT(num_identification_disagreements)), 2) as id_disagree_pct,
ROUND(AVG(description_length),2) as avg_description_len,
SUM(expert_match) AS expert_match_count,
ROUND(100 * (SUM(expert_match) / observations_count),2) as expert_match_pct,
ROUND(100.0000 * (SUM(expert_match) / SUM(COUNT(*)) OVER ()), 3) AS expert_match_total_pct

FROM preprocessed.gbif_citizen_labeled g
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
FROM features.observer_species_obs s
WHERE o.recordedBy = s.recordedBy;
