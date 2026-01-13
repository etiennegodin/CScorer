CREATE OR REPLACE VIEW score.expert_match AS

-- botanist with > 1000 obs 
WITH experts_ids AS(

    SELECT c.gbifID,
    1.0 AS expert_match_score 


    FROM preprocessed.gbif_citizen c
    JOIN features.community_validation v
        ON c."gbifID" = v."gbifID"
    WHERE v.cmva_expert_id = 1 

),

experienced_obs AS(

    SELECT c."gbifID",
    0.75 AS expert_match_score 

    FROM preprocessed.gbif_citizen c
    JOIN observers.inat_data o
        ON o.name = c."recordedBy"
    WHERE o.observations_count > 1000 AND o.identifications_count > 500
),


casual_obs AS(

    SELECT c."gbifID",
    0.5 AS expert_match_score 

    FROM preprocessed.gbif_citizen c
    JOIN observers.inat_data o
        ON o.name = c."recordedBy"
    WHERE o.observations_count > 500 AND o.identifications_count > 50
),

community_agreement AS (


    SELECT c."gbifID",
    0.25 AS expert_match_score 

    FROM preprocessed.gbif_citizen c
    JOIN features.community_validation v
        ON c."gbifID" = v."gbifID"
    WHERE v.cmva_id_count >= 3 AND v.cmva_id_agree_rate = 1.0 
)


SELECT c."gbifID",
COALESCE(e.expert_match_score, ep.expert_match_score, co.expert_match_score, ca.expert_match_score, 0.0) AS expert_match_score


FROM preprocessed.gbif_citizen c
LEFT JOIN experts_ids e ON c."gbifID" = e."gbifID"
LEFT JOIN experienced_obs ep ON c."gbifID" = ep."gbifID"
LEFT JOIN casual_obs co ON c."gbifID" = co."gbifID"
LEFT JOIN community_agreement ca ON c."gbifID" = ca."gbifID"











