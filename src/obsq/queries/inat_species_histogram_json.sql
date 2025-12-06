CREATE OR REPLACE VIEW clean.inat_histogram AS
SELECT 
    item_key as taxonID,

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
    CAST(json_extract(json, '$.month_of_year."12"') AS INTEGER) AS month_12,
    (month_1+month_2+month_3+month_4+month_5+month_6+month_7+month_8+month_9+month_10+month_11+month_12) AS count

FROM raw.inat_phenology
ORDER BY taxonID
;
