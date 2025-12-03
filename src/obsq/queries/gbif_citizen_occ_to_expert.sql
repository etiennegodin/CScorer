-- create gbif_citizen view
CREATE OR REPLACE VIEW clean.gbif_citizen_no_expert AS
SELECT *
FROM clean.gbif_citizen
WHERE recordedBy NOT IN (SELECT recordedBy FROM raw.citizen_expert) AND institutionCode = 'iNaturalist'
;

-- create gbif_expert_table to insert into new observations
CREATE OR REPLACE TABLE preprocessed.gbif_expert AS
SELECT * FROM clean.gbif_expert;

-- CREATE VIEW OF observations from citizen expert 
CREATE OR REPLACE VIEW raw.citizen_occ_from_expert AS
SELECT *
FROM clean.gbif_citizen
WHERE recordedBy IN (SELECT recordedBy FROM raw.citizen_expert);

-- add citizen_occ_from_expert VIEW TO TABLE
INSERT INTO preprocessed.gbif_expert
SELECT * FROM raw.citizen_occ_from_expert;

