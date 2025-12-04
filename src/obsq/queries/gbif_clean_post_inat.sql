CREATE OR REPLACE VIEW preprocessed.gbif_post_inat_observers AS
SELECT *
FROM preprocessed.gbif_citizen_no_expert
WHERE "user_id" IN (
                    SELECT "id" 
                    FROM observers.inat_data 
);


