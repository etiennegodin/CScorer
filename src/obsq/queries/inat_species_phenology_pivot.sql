CREATE OR REPLACE TABLE preprocessed.inat_phenology_pivoted AS

SELECT
    taxonID,
    attribute_label,
    value_label,
    count,
    CAST(month_1 AS FLOAT) / NULLIF(count, 0) AS month_1_density,
    CAST(month_2 AS FLOAT) / NULLIF(count, 0) AS month_2_density,
    CAST(month_3 AS FLOAT) / NULLIF(count, 0) AS month_3_density,
    CAST(month_4 AS FLOAT) / NULLIF(count, 0) AS month_4_density,
    CAST(month_5 AS FLOAT) / NULLIF(count, 0) AS month_5_density,
    CAST(month_6 AS FLOAT) / NULLIF(count, 0) AS month_6_density,
    CAST(month_7 AS FLOAT) / NULLIF(count, 0) AS month_7_density,
    CAST(month_8 AS FLOAT) / NULLIF(count, 0) AS month_8_density,
    CAST(month_9 AS FLOAT) / NULLIF(count, 0) AS month_9_density,
    CAST(month_10 AS FLOAT) / NULLIF(count, 0) AS month_10_density,
    CAST(month_11 AS FLOAT) / NULLIF(count, 0) AS month_11_density,
    CAST(month_12 AS FLOAT) / NULLIF(count, 0) AS month_12_density,
    month_1,
    month_2,
    month_3,
    month_4,
    month_5,
    month_6,
    month_7,
    month_8,
    month_9,
    month_10,
    month_11,
    month_12
FROM clean.inat_phenology 
ORDER BY taxonID, attribute_label, value_label