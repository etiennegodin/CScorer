CREATE OR REPLACE VIEW features.phenology AS

SELECT
    g.gbifID,
    p.count AS phenology_total_count,
    -- Raw month counts
    CASE g.month
        WHEN 1 THEN p.month_1
        WHEN 2 THEN p.month_2
        WHEN 3 THEN p.month_3
        WHEN 4 THEN p.month_4
        WHEN 5 THEN p.month_5
        WHEN 6 THEN p.month_6
        WHEN 7 THEN p.month_7
        WHEN 8 THEN p.month_8
        WHEN 9 THEN p.month_9
        WHEN 10 THEN p.month_10
        WHEN 11 THEN p.month_11
        WHEN 12 THEN p.month_12
    END AS phenology_month_count,
    -- Month density (normalized by total count)
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
    END AS phenology_month_density
FROM labeled.gbif_citizen g
LEFT JOIN preprocessed.inat_phenology_pivoted p
    ON g.taxonID = p.taxonID
    AND p.attribute_label IS NOT NULL
ORDER BY g.gbifID, g.taxonID, p.attribute_label, p.value_label
