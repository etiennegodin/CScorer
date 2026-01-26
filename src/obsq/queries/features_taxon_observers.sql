CREATE OR REPLACE VIEW features.observers_taxon_id AS

SELECT g."taxonID", g."identifiedBy",

COUNT(g."gbifID") as taxon_id_count

FROM preprocessed.gbif_citizen g

GROUP BY g."taxonID", g."identifiedBy"
ORDER BY g."identifiedBy" ASC