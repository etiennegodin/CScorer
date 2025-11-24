-- create gbif_citizen table
CREATE OR REPLACE TABLE preprocessed.gbif_citizen AS
SELECT *
FROM preprocessed.gbif_citizen_prep
WHERE recordedBy NOT IN (SELECT recordedBy FROM preprocessed.inat_expert);

-- create gbif_expert_table
CREATE OR REPLACE TABLE preprocessed.gbif_expert AS
SELECT * FROM clean.gbif_expert;

-- CREATE VIEW OF obsevations from inat expert 
CREATE OR REPLACE VIEW preprocessed.expert_occ_inat AS
SELECT *
FROM clean.gbif_citizen
WHERE recordedBy IN (SELECT recordedBy FROM preprocessed.inat_expert);

-- add expert_occ_inat VIEW TO TABLE
INSERT INTO preprocessed.gbif_expert
SELECT * FROM preprocessed.expert_occ_inat;

