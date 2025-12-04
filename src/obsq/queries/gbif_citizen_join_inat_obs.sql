CREATE OR REPLACE TABLE preprocessed.preprocess.gbif_citizen_inat AS
SELECT g.*,
o.user_login,
o.user_id,
o.num_identification_agreements,
o.num_identification_disagreements


FROM clean.gbif_citizen_no_expert g
JOIN clean.inat_observations o
    ON g."occurrenceID" = o.url

WHERE g."recordedBy" NOT IN(
                    SELECT id_string 
                    FROM raw.discarded_inat_observers
);
