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
g.coordinateUncertaintyInMeters,
g.decimalLatitude,
g.decimalLongitude,
g.issue,
g.geom,


FROM {{source_table_name}} g
WHERE g.coordinateUncertaintyInMeters < 1000 OR g.coordinateUncertaintyInMeters IS NULL AND g.taxonRank = 'SPECIES' 

