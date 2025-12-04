ALTER TABLE raw.discarded_inat_observers ADD COLUMN IF NOT EXISTS obs_count INTEGER;
UPDATE raw.discarded_inat_observers t1
SET obs_count = 
(
    SELECT COUNT(*) 
    FROM clean.gbif_citizen_no_expert t2
    WHERE t1.id_string = t2."recordedBy"
);

CREATE OR REPLACE TABLE preprocessed.gbif_citizen AS
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


