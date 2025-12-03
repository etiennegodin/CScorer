DELETE FROM preprocessed.gbif_citizen
WHERE "recordedBy" IN(
                    SELECT id_string 
                    FROM raw.discarded_inat_observers 

);
