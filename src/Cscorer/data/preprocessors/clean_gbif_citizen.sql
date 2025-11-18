CREATE OR REPLACE TABLE preprocessed.gbif_citizen AS 

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

i.url,
CASE
    WHEN i.description IS NOT NULL THEN 1
    ELSE 0
END AS description,
i.num_identification_agreements,
i.num_identification_disagreements,
i.image_url,


FROM gbif_raw.citizen g
JOIN inat.occurences i
ON g.occurrenceID = i.url
WHERE g.institutionCode = 'iNaturalist' AND g.taxonRank = 'SPECIES'

