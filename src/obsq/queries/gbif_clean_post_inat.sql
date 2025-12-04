ALTER TABLE raw.discarded_inat_observers ADD COLUMN IF NOT EXISTS obs_count INTEGER;
UPDATE raw.discarded_inat_observers t1
SET obs_count = 
(
    SELECT COUNT(*) 
    FROM clean.gbif_citizen_no_expert t2
    WHERE t1.id_string = t2."recordedBy"
);

CREATE OR REPLACE TABLE preprocessed.gbif_citizen AS
SELECT *
FROM clean.gbif_citizen_no_expert
WHERE "recordedBy" NOT IN(
                    SELECT id_string 
                    FROM raw.discarded_inat_observers
);


