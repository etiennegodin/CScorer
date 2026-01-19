CREATE OR REPLACE VIEW features.taxonomic AS




SELECT g.taxonID,

--ROUND(COUNT(g.taxonID) /  SUM(COUNT(DISTINCT g.taxonID)) OVER (),3) as taxo_freq,
(COUNT(DISTINCT s.similar_taxon)) as taxo_confusability_index

FROM preprocessed.gbif_citizen g
LEFT JOIN clean.inat_similar_species s
    ON g."taxonID" = s."taxonID"
GROUP BY g.taxonID
;

