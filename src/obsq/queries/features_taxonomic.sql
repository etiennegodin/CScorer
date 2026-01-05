CREATE OR REPLACE VIEW features.taxonomic AS


SELECT "taxonID",

ROUND(COUNT(g.taxonID) /  SUM(COUNT(DISTINCT g.taxonID)) OVER (),3) as taxo_freq


FROM labeled.gbif_citizen g
GROUP BY g.taxonID
;

SELECT g.taxonID,
ROUND(COUNT(DISTINCT g.taxonID) / SUM(COUNT(DISTINCT g.taxonID)) OVER (), 5) AS obsv_total_pct,
ROUND(COUNT(g.taxonID) / SUM(COUNT(DISTINCT g.taxonID)) OVER (), 3) AS taxo_frequency,
COUNT(s.similar_taxon) as taxo_similar_taxon
FROM labeled.gbif_citizen g
JOIN clean.inat_similar_species s
    ON g."taxonID" = s."taxonID"
GROUP BY g.taxonID