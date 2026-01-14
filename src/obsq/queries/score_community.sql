CREATE OR REPLACE VIEW score.community AS


SELECT "gbifID", 

ROUND(cmva_id_count / 10 * 0.4 + cmva_id_agree_rate * 0.3 + cmva_expert_id * 0.3, 3) AS community_score



FROM features.community_validation 




