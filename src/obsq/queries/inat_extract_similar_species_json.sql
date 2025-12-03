CREATE OR REPLACE VIEW clean.inat_similar_species AS
SELECT 
    id_string as taxonID,
    CAST(json_extract(json, '$.count') AS INTEGER) AS count,
    
    -- controlled_attribute fields
    CAST(json_extract(json, '$.taxon.id') AS INTEGER) AS similar_taxon,

FROM raw.inat_similar_species
ORDER BY taxonID
;

