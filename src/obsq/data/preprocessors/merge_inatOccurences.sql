CREATE OR REPLACE TABLE preprocessed.gbif_citizen AS 

WITH inat_inter AS (

SELECT 
observed_on,
url, 
image_url,
num_identification_agreements,
num_identification_disagreements,
"description",
LEN("description") as description_length
FROM raw.inat_occurences
),

gbif_temp AS(


SELECT * FROM clean.gbif_citizen

)

SELECT *

FROM gbif_temp g
JOIN inat_inter i
    ON g.occurrenceID = i.url;
