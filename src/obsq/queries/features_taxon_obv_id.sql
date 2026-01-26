CREATE OR REPLACE VIEW features.observers_taxon_id AS

WITH species_count AS(

    SELECT "identifiedBy", "taxonID", COUNT(*) AS count
    FROM preprocessed.gbif_citizen
    GROUP BY "identifiedBy", taxonID
)

SELECT s."identifiedBy", s.taxonID,
ROUND(CAST(s.count AS DECIMAL(10, 4)) / SUM(s.count) OVER (PARTITION BY s."identifiedBy"),5) AS species_prob
FROM species_count s
ORDER BY "identifiedBy"


