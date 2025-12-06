CREATE OR REPLACE TABLE preprocessed.gbif_citizen AS
SELECT *
FROM preprocessed.gbif_citizen_no_expert
WHERE "user_id" IN (
                    SELECT "id" 
                    FROM observers.inat_data 
);


