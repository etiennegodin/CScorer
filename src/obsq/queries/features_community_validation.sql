CREATE OR REPLACE VIEW features.community_validation AS
SELECT gbifID,
(num_identification_agreements + num_identification_disagreements) AS cmva_id_count,
ROUND(num_identification_agreements / (num_identification_agreements + num_identification_disagreements), 4) as cmva_id_agree_rate,
CAST("dateIdentified" AS DATE) - CAST("eventDate" AS DATE) as cmva_id_time,

CASE
    WHEN identifiedByID IS NULL THEN 0
    ELSE 1
END AS expert_id

FROM labeled.gbif_citizen



