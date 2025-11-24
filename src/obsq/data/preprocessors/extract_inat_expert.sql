CREATE OR REPLACE VIEW preprocessed.inat_expert AS

WITH inat_obs AS (

SELECT 
    "json" ->> 'id' AS inat_id,
    "json" ->> 'name' AS inat_name,
    "json" ->> 'orcid' AS orcid,
    1 AS id

FROM raw.inat_observers
WHERE orcid is NOT NULL
    OR inat_name IN ('Jacques Ranger')

),

expert_in_inat AS( -- name in expert occurences found in inat_citizen

SELECT recordedBy,
    2 AS id

FROM clean.gbif_expert
WHERE recordedBy IN (SELECT recordedBy FROM clean.gbif_citizen)


)

SELECT i.inat_name, i.id FROM inat_obs i
UNION ALL             -- keeps duplicates (most common)
SELECT e.recordedBy, e.id FROM expert_in_inat e;

