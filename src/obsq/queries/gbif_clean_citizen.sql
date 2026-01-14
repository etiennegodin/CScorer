CREATE OR REPLACE TABLE clean.gbif_citizen AS 

WITH cast_json AS(
SELECT "gbifID",
CAST(g.dynamicProperties AS JSON) as annotations

FROM raw.gbif_citizen g
WHERE institutionCode = 'iNaturalist'

),

json_extracted AS(

SELECT t."gbifID", e.key, e.value
FROM cast_json t,
     json_each(try_cast(t.annotations AS JSON)) e
),

json_first_only AS(

    SELECT gbifID,
CASE
  WHEN NOT json_valid(value) THEN NULL
  WHEN json_type(value) = 'ARRAY'
       THEN json_extract(value, '$[0]')
  ELSE value
END AS value_cleaned
FROM json_extracted
),


main_cleanup AS(
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
g.decimalLatitude,
g.decimalLongitude,
g.iucnRedListCategory,
g.issue,
g.hasCoordinate,
g.geom,
CASE g.sex
    WHEN 'Male' THEN 11
    WHEN 'Female' THEN 10
    ELSE NULL
END AS pheno_sex,
CASE reproductiveCondition
    WHEN 'flower buds' THEN 15
    WHEN 'flowers|flower buds' THEN 15
    WHEN 'no flowers or fruits' THEN 21
    WHEN 'flowers' THEN 13
    WHEN 'fruits or seeds' THEN 14
    ELSE NULL
END AS pheno_repro,
CASE replace(j.value_cleaned, '"', '')
    WHEN 'no live leaves' THEN 40
    WHEN 'green leaves' THEN 38
    WHEN 'breaking leaf buds' THEN 37
    WHEN 'colored leaves' THEN 39
    ELSE NULL
END AS pheno_leaves,
CASE
    WHEN coordinateUncertaintyInMeters IS NULL THEN 0
    ELSE CAST(coordinateUncertaintyInMeters AS INT)
END AS coordinateUncertaintyInMeters,

CASE
    WHEN "mediaType" IS NULL THEN 0
    ELSE CAST((length(mediaType) - length(REPLACE(mediaType, 'StillImage', ''))) /10 AS INTEGER) 
END AS media_count

FROM raw.gbif_citizen g
LEFT JOIN json_first_only j
    ON g."gbifID" = j.gbifID
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







