CREATE OR REPLACE TABLE preprocessed.gbif_expert AS 

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


FROM gbif_raw.expert g
WHERE g.institutionCode = 'iNaturalist'

