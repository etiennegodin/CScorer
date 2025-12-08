CREATE OR REPLACE VIEW preprocessed.gbif_citizen_inat AS
SELECT g.*,
o.user_login,
o.user_id,
o.num_identification_agreements,
o.num_identification_disagreements


FROM clean.gbif_citizen g
JOIN clean.inat_observations o
    ON g."occurrenceID" = o.url

WHERE o.num_identification_agreements IS NOT NULL
AND o.num_identification_disagreements IS NOT NULL

;
