CREATE OR REPLACE VIEW features.observers_taxon_obsv AS

WITH species_count AS(

    SELECT "recordedBy", "taxonID", COUNT(*) AS count
    FROM preprocessed.gbif_citizen
    GROUP BY "recordedBy", taxonID
)

SELECT s."recordedBy", s.taxonID,
ROUND(CAST(s.count AS DECIMAL(10, 4)) / SUM(s.count) OVER (PARTITION BY s."recordedBy"),5) AS species_prob
FROM species_count s
ORDER BY recordedBy



