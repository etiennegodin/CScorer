CREATE OR REPLACE VIEW clean.inat_histogram AS
SELECT 
    item_key as taxonID,
    -- month_of_year unpivoted
    CAST(json_extract(json, '$.month_of_year."1"') AS INTEGER) AS "1",
    CAST(json_extract(json, '$.month_of_year."2"') AS INTEGER) AS "2",
    CAST(json_extract(json, '$.month_of_year."3"') AS INTEGER) AS "3",
    CAST(json_extract(json, '$.month_of_year."4"') AS INTEGER) AS "4",
    CAST(json_extract(json, '$.month_of_year."5"') AS INTEGER) AS "5",
    CAST(json_extract(json, '$.month_of_year."6"') AS INTEGER) AS "6",
    CAST(json_extract(json, '$.month_of_year."7"') AS INTEGER) AS "7",
    CAST(json_extract(json, '$.month_of_year."8"') AS INTEGER) AS "8",
    CAST(json_extract(json, '$.month_of_year."9"') AS INTEGER) AS "9",
    CAST(json_extract(json, '$.month_of_year."10"') AS INTEGER) AS "10",
    CAST(json_extract(json, '$.month_of_year."11"') AS INTEGER) AS "11",
    CAST(json_extract(json, '$.month_of_year."12"') AS INTEGER) AS "12"

FROM raw.inat_histogram
ORDER BY taxonID
;
