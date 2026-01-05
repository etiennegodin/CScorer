-- create table from gbif_citizen_inat without expert observations
CREATE OR REPLACE TABLE preprocessed.gbif_citizen_no_expert AS
SELECT *
FROM preprocessed.gbif_citizen_inat
WHERE recordedBy NOT IN (SELECT recordedBy FROM observers.expert)
;

-- create base gbif_expert table (table instaed of TABLE to insert into)
CREATE OR REPLACE TABLE preprocessed.gbif_expert AS
SELECT *
FROM clean.gbif_expert;

-- CREATE TABLE OF observations from citizen expert 
CREATE OR REPLACE TABLE preprocessed.citizen_obs_from_expert AS
SELECT *
FROM preprocessed.gbif_citizen_inat
WHERE recordedBy IN (SELECT recordedBy FROM observers.expert);

-- add citizen_occ_from_expert VIEW TO TABLE
INSERT INTO preprocessed.gbif_expert
SELECT * EXCLUDE (user_login,
                    user_id,
                    num_identification_agreements,
                    num_identification_disagreements,
                    media_count
                    )
FROM preprocessed.citizen_obs_from_expert;

