CREATE OR REPLACE VIEW preprocessed.inat_similar_species AS

WITH uniques AS (

    SELECT DISTINCT item_key, * raw.inat_similar_species
)


SELECT 
    item_key as taxonID,
    CAST(json_extract(json, '$.count') AS INTEGER) AS count,
    
    -- controlled_attribute fields
    CAST(json_extract(json, '$.taxon.id') AS INTEGER) AS similar_taxon,

FROM uniques
ORDER BY taxonID
;

