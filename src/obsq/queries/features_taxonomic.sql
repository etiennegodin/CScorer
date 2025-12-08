CREATE OR REPLACE VIEW features.taxonomic AS

SELECT g.taxonID,

COUNT(*) as count,
ROUND(COUNT(g.taxonID) / SUM(COUNT(DISTINCT g.taxonID)) OVER (), 5) AS frequency,
COUNT(s.similar_taxon) as similar_taxon
FROM labeled.gbif_citizen g
JOIN clean.inat_similar_species s
    ON g."taxonID" = s."taxonID"
GROUP BY g.taxonID