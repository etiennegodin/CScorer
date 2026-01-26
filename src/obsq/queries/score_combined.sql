CREATE OR REPLACE VIEW score.combined AS

SELECT c.gbifID,
s.spatial_score,
p.pheno_score,
o.observer_score,
i.identifier_score,
e.expert_id_score 

FROM features.combined_p c
JOIN score.spatial s ON c."gbifID" = s."gbifID"
JOIN score.phenology p ON c."gbifID" = p."gbifID"
JOIN score.observer o ON c."gbifID" = o."gbifID"
JOIN score.identifier i ON c."gbifID" = i."gbifID"
JOIN score.expert_id e ON c."gbifID" = e."gbifID"

