CREATE OR REPLACE VIEW clean.inat_phenology AS
SELECT 
    id_string as taxonID,
    CAST(json_extract(json, '$.count') AS INTEGER) AS count,
    
    -- controlled_attribute fields
    CAST(json_extract(json, '$.controlled_attribute.id') AS INTEGER) AS attribute_id,
    json_extract_string(json, '$.controlled_attribute.label') AS attribute_label,
    
    -- controlled_value fields
    CAST(json_extract(json, '$.controlled_value.id') AS INTEGER) AS value_id,
    json_extract_string(json, '$.controlled_value.label') AS value_label,
    
    -- month_of_year unpivoted
    CAST(json_extract(json, '$.month_of_year."1"') AS INTEGER) AS month_1,
    CAST(json_extract(json, '$.month_of_year."2"') AS INTEGER) AS month_2,
    CAST(json_extract(json, '$.month_of_year."3"') AS INTEGER) AS month_3,
    CAST(json_extract(json, '$.month_of_year."4"') AS INTEGER) AS month_4,
    CAST(json_extract(json, '$.month_of_year."5"') AS INTEGER) AS month_5,
    CAST(json_extract(json, '$.month_of_year."6"') AS INTEGER) AS month_6,
    CAST(json_extract(json, '$.month_of_year."7"') AS INTEGER) AS month_7,
    CAST(json_extract(json, '$.month_of_year."8"') AS INTEGER) AS month_8,
    CAST(json_extract(json, '$.month_of_year."9"') AS INTEGER) AS month_9,
    CAST(json_extract(json, '$.month_of_year."10"') AS INTEGER) AS month_10,
    CAST(json_extract(json, '$.month_of_year."11"') AS INTEGER) AS month_11,
    CAST(json_extract(json, '$.month_of_year."12"') AS INTEGER) AS month_12

FROM raw.inat_phenology
ORDER BY taxonID attribute_id, value_id
;
