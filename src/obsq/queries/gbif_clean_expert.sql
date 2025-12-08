CREATE OR REPLACE VIEW clean.gbif_expert AS 

WITH main_cleanup AS(
SELECT g.gbifID,
g.occurrenceID,
g.year,
g.month,
g.day,
g.recordedBy,
g.recordedByID,
g.occurrenceRemarks,
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
g.species,
g.taxonRank,
g.taxonID,
g.sex,
g.reproductiveCondition,
g.dynamicProperties as annotations,
g.decimalLatitude,
g.decimalLongitude,
occurrenceStatus,
g.iucnRedListCategory,
g.issue,
g.hasCoordinate,
g.geom,
g.coordinateUncertaintyInMeters

FROM raw.gbif_expert g
)

SELECT *



FROM main_cleanup m

WHERE m.taxonRank = 'SPECIES' 
AND m.month >= 4
AND m.month < 9
AND m.hasCoordinate = True
;            







