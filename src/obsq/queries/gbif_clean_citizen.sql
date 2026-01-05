CREATE OR REPLACE VIEW clean.gbif_citizen AS 

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
CAST(g.dynamicProperties AS JSON) as annotations,
g.decimalLatitude,
g.decimalLongitude,
occurrenceStatus,
g.iucnRedListCategory,
g.issue,
g.hasCoordinate,
g.geom,
CASE
    WHEN coordinateUncertaintyInMeters IS NULL THEN 0
    ELSE CAST(coordinateUncertaintyInMeters AS INT)
END AS coordinateUncertaintyInMeters,

CASE
    WHEN "mediaType" IS NULL THEN 0
    ELSE CAST((length(mediaType) - length(REPLACE(mediaType, 'StillImage', ''))) AS INTEGER)
END AS media_count

FROM raw.gbif_citizen g
)

SELECT *



FROM main_cleanup m

WHERE m.coordinateUncertaintyInMeters < 3000 
AND m.taxonRank = 'SPECIES' 
AND m.month >= 4
AND m.month < 9
AND m.hasCoordinate = True
AND m.taxonID IS NOT NULL
AND institutionCode = 'iNaturalist'
AND dateIdentified IS NOT NULL     
;            







