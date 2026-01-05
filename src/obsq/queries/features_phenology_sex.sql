CREATE OR REPLACE VIEW features.phenology_sex AS

SELECT
    g.gbifID,
    CASE g.month
        WHEN 1 THEN p.month_1_density
        WHEN 2 THEN p.month_2_density
        WHEN 3 THEN p.month_3_density
        WHEN 4 THEN p.month_4_density
        WHEN 5 THEN p.month_5_density
        WHEN 6 THEN p.month_6_density
        WHEN 7 THEN p.month_7_density
        WHEN 8 THEN p.month_8_density
        WHEN 9 THEN p.month_9_density
        WHEN 10 THEN p.month_10_density
        WHEN 11 THEN p.month_11_density
        WHEN 12 THEN p.month_12_density
    END AS pheno_sex_month_density


FROM labeled.gbif_citizen g

JOIN preprocessed.inat_phenology_pivoted p
    ON g."taxonID" = p."taxonID"
    AND p.value_id = g.pheno_sex

ORDER BY g.gbifID, g.taxonID, p.value_id
