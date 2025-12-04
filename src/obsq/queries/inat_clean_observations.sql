CREATE OR REPLACE VIEW clean.inat_observations AS

SELECT id,
url,
user_login,
user_id,
num_identification_agreements,
num_identification_disagreements

FROM raw.inat_observations
WHERE quality_grade = 'research';

