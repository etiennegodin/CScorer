CREATE OR REPLACE VIEW clean.{{target_table_name}} AS 

WITH main_cleanup AS(
SELECT g.gbifID,
g.occurrenceID,
g.publishingOrgKey,
g.eventDate,
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
g.decimalLatitude,
g.decimalLongitude,
g.issue,
g.geom,
CASE
    WHEN coordinateUncertaintyInMeters IS NULL THEN 0
    ELSE coordinateUncertaintyInMeters
END AS coordinateUncertaintyInMeters,
(
    length(g.mediaType) 
  - length(replace(g.mediaType, 'StillImage', ''))
) / length('StillImage') AS media_count


FROM {{source_table_name}} g
)

SELECT * FROM main_cleanup m

WHERE m.coordinateUncertaintyInMeters < 5000 
AND m.taxonRank = 'SPECIES' 
AND m.month > 4 
AND m.month < 9 
AND m.publishingOrgKey = '28eb1a3f-1c15-4a95-931a-4af90ecb574d';







