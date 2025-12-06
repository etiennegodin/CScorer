CREATE OR REPLACE VIEW features.metadata AS
SELECT gbifID,
CAST(media_count AS INTEGER) AS media_count,
"coordinateUncertaintyInMeters",

CASE
    WHEN reproductiveCondition IS NULL THEN 0
    ELSE 1
END AS pheno_01,

CASE
    WHEN annotations IS NULL THEN 0
    ELSE 1
END AS pheno_02,

CASE
    WHEN sex IS NULL THEN 0
    ELSE 1
END AS pheno_03,

CASE
    WHEN occurrenceRemarks IS NULL THEN 0
    ELSE LENGTH("occurrenceRemarks")
END AS occurrenceRemarks,



FROM labeled.gbif_citizen



