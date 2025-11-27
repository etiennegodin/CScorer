CREATE OR REPLACE VIEW clean.{{target_table_name}} AS 

WITH main_cleanup AS(
SELECT g.gbifID,
g.occurrenceID,
g."references" as "url",
g.recordedBy,
g.recordedByID,
g.eventDate,
g.identificationID,
g.identifiedBy,
g.identifiedByID,
g.identificationRemarks,
g.dateIdentified,
g.catalogNumber,
g.institutionCode,
g.kingdom,
g.phylum,
g.class,
g.order,
g.family,
g.genus,
g.scientificName,
g.taxonRank,
g.taxonID,
g.sex,
g.reproductiveCondition,
g.dynamicProperties as annotations,
g.decimalLatitude,
g.decimalLongitude,
occurrenceStatus,
g.geodeticDatum,
g.geom,
CASE
    WHEN coordinateUncertaintyInMeters IS NULL THEN 0
    ELSE CAST(coordinateUncertaintyInMeters AS INT)
END AS coordinateUncertaintyInMeters,


FROM {{source_table_name}} g
)

SELECT * FROM main_cleanup m

WHERE m.coordinateUncertaintyInMeters < 3000 
AND m.taxonRank = 'species' 
AND MONTH(m.eventDate) > MONTH(CAST('2000-04-01' AS DATE))
AND MONTH(m.eventDate) < MONTH(CAST('2000-09-01' AS DATE))
AND m.institutionCode = 'iNaturalist';







