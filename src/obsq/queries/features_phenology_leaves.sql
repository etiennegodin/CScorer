CREATE OR REPLACE VIEW features.phenology_leaves AS

SELECT
    g.gbifID,
    COALESCE(

        CASE 
            WHEN g.month = 1 THEN p.month_1_density
            WHEN g.month = 2 THEN p.month_2_density
            WHEN g.month = 3 THEN p.month_3_density
            WHEN g.month = 4 THEN p.month_4_density
            WHEN g.month = 5 THEN p.month_5_density
            WHEN g.month = 6 THEN p.month_6_density
            WHEN g.month = 7 THEN p.month_7_density
            WHEN g.month = 8 THEN p.month_8_density
            WHEN g.month = 9 THEN p.month_9_density
            WHEN g.month = 10 THEN p.month_10_density
            WHEN g.month = 11 THEN p.month_11_density
            WHEN g.month = 12 THEN p.month_12_density
        END, 
        0.0) AS pheno_leaves_month_density


FROM preprocessed.gbif_citizen g

LEFT JOIN preprocessed.inat_phenology_pivoted p
    ON g."taxonID" = p."taxonID"
    AND p.value_id = g.pheno_leaves

ORDER BY g.gbifID, g.taxonID, p.value_id
