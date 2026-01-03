CREATE OR REPLACE VIEW features.metadata AS
SELECT gbifID,
CAST(media_count AS INTEGER) AS meta_media_count,
"coordinateUncertaintyInMeters" AS meta_coordinateUncertaintyInMeters,

CASE
    WHEN reproductiveCondition IS NULL THEN 0
    ELSE 1
END AS meta_pheno_01,

CASE
    WHEN annotations IS NULL THEN 0
    ELSE 1
END AS meta_pheno_02,

CASE
    WHEN sex IS NULL THEN 0
    ELSE 1
END AS meta_pheno_03,

CASE
    WHEN occurrenceRemarks IS NULL THEN 0
    ELSE LENGTH("occurrenceRemarks")
END AS meta_occurrenceRemarks,



FROM labeled.gbif_citizen



