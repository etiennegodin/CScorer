CREATE OR REPLACE TABLE preprocessed.{{target_table_name}} AS 

SELECT g.gbifID,
g.occurrenceID,
g.publishingOrgKey,
g.kingdom,
g.phylum,
g.class,
g.order,
g.family,
g.genus,
g.species,
g.taxonRank,
g.taxonKey,
g.scientificName,
g.eventDate,
g.day,
g.month,
g.year,
g.recordedBy,
g.mediaType,
g.decimalLatitude,
g.decimalLongitude,
g.issue,
g.geom,
CASE
    WHEN coordinateUncertaintyInMeters IS NULL THEN 0
    ELSE coordinateUncertaintyInMeters
END AS coordinateUncertaintyInMeters


FROM {{source_table_name}} g
WHERE g.coordinateUncertaintyInMeters < 1000 OR g.coordinateUncertaintyInMeters IS NULL;


-- FILTERS 

-- species only
DELETE FROM preprocessed.{{target_table_name}} t
WHERE NOT t.taxonRank = 'SPECIES';

-- growing season
DELETE FROM preprocessed.{{target_table_name}} t
WHERE t.month < 4;

DELETE FROM preprocessed.{{target_table_name}} t
WHERE t.month > 9;







