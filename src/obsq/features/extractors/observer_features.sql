
CREATE OR REPLACE VIEW features.all_observers AS


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

gbif_metadata  AS (

SELECT
COUNT(*) as observations_count,
ROUND(100.0000 * COUNT(*) / SUM(COUNT(*)) OVER (), 3) AS total_pct,
COUNT(DISTINCT class) as class_count,
COUNT(DISTINCT "order") as order_count,
COUNT(DISTINCT family) as family_count,
COUNT(DISTINCT genus) as genus_count,
COUNT(DISTINCT species) as species_count,
COUNT(DISTINCT "day") as unique_day_count, -- better way to find distribution 
COUNT(DISTINCT "month") as unique_month_count,
COUNT(DISTINCT "year") as unique_year_count, 
COUNT(DISTINCT cast(eventDate AS DATE)) as unique_dates,
ROUND(AVG(coordinateUncertaintyInMeters),2) as avg_coord_un,
MAX(coordinateUncertaintyInMeters) as max_coord_un,
ROUND(AVG(media_count),2) as avg_media_count,
SUM(num_identification_agreements) as id_agree_count,
ROUND(SUM(num_identification_agreements) / COUNT(num_identification_agreements),2)as id_agree_pct,
SUM(num_identification_disagreements) as id_disagree_count,
ROUND((SUM(num_identification_disagreements) / COUNT(num_identification_disagreements)), 2) as id_disagree_pct,
ROUND(AVG(description_length),2) as avg_description_len,


recordedBy

FROM preprocessed.gbif_citizen_prep

GROUP BY recordedBy

)

SELECT g.*, i.* EXCLUDE (i.inat_name)

FROM gbif_metadata g
JOIN inat_data i
    ON g.recordedBy = i.inat_name;

CREATE OR REPLACE TABLE features.observers AS
SELECT * FROM features.all_observers WHERE orcid is NULL;



