CREATE OR REPLACE TABLE clean.gbif_expert AS 

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
g.decimalLatitude,
g.decimalLongitude,
g.iucnRedListCategory,
g.issue,
g.hasCoordinate,
g.geom,
g.sex AS pheno_sex,
g.reproductiveCondition AS pheno_repro,
g.dynamicProperties AS pheno_leaves,
g.coordinateUncertaintyInMeters,
g.mediaType AS media_count


FROM raw.gbif_expert g
)

SELECT *



FROM main_cleanup m

WHERE m.taxonRank = 'SPECIES' 
AND m.month >= 4
AND m.month < 9
AND m.hasCoordinate = True
;            







