CREATE OR REPLACE VIEW preprocessed.gbif_citizen_inat AS
SELECT g.*,
o.user_login,
o.user_id,
o.num_identification_agreements,
o.num_identification_disagreements


FROM clean.gbif_citizen g
JOIN clean.inat_observations o
    ON g."occurrenceID" = o.url

;
