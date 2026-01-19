CREATE OR REPLACE VIEW score.main AS

SELECT g.gbifID,
(0.3 * s.spatial_score + 0.15 * p.pheno_score + 0.25 * o.observer_score + 0.25 *  i.identifier_score + 3 * e.expert_id_score ) as score





FROM preprocessed.gbif_citizen g
JOIN score.spatial s ON g."gbifID" = s."gbifID"
JOIN score.phenology p ON g."gbifID" = p."gbifID"
JOIN score.observer o ON g."gbifID" = o."gbifID"
JOIN score.identifier i ON g."gbifID" = i."gbifID"
JOIN score.expert_id e ON g."gbifID" = e."gbifID"

