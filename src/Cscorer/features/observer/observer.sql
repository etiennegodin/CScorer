
CREATE OR REPLACE TABLE features.observers AS


WITH inat_data AS (

SELECT 
    "json" ->> 'id' AS id,
    "json" ->> 'login' AS login,
    "json" ->> 'orcid' AS orcid,
    "json" ->> 'observations_count' AS inat_obs_count,
    "json" ->> 'identifications_count' AS id_count,
    "json" ->> 'species_count' AS inat_species_count,


FROM inat.observers

),

gbif_metadata  AS (

SELECT
COUNT(*) as observations_count,
ROUND(100.0000 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS total_pct,
COUNT(DISTINCT class) as class_count,
COUNT(DISTINCT "order") as order_count,
COUNT(DISTINCT family) as family_count,
COUNT(DISTINCT genus) as genus_count,
COUNT(DISTINCT species) as species_count,
COUNT(DISTINCT "day") as unique_day_count,
COUNT(DISTINCT "month") as unique_month_count,
COUNT(DISTINCT "year") as unique_year_count,
AVG(coordinateUncertaintyInMeters) as avg_coord_un,
MAX(coordinateUncertaintyInMeters) as max_coord_un,
SUM(num_identification_agreements) as id_agree_count,
100 * SUM(num_identification_agreements) / SUM(SUM(num_identification_agreements)) OVER () as id_agree_pct,
SUM(num_identification_disagreements) as id_disagree_count,
100 * SUM(num_identification_disagreements) / SUM(SUM(num_identification_disagreements)) OVER () as id_disagree_pct,
100 * SUM(description) / observations_count as description_pct,
100 * SUM(expert_match) / observations_count as expert_match_pct,
recordedBy

FROM preprocessed.citizen_matched

GROUP BY recordedBy

)

SELECT g.*, i.* 

FROM gbif_metadata g
JOIN inat_data i
    ON g.recordedBy = i.login




