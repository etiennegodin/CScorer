CREATE OR REPLACE TABLE score.expert_id AS
SELECT g.gbifID,

CASE 
    WHEN g.identifiedByID IS NOT NULL THEN 1
    ELSE 0
END AS expert_id_score

FROM preprocessed.gbif_citizen g
