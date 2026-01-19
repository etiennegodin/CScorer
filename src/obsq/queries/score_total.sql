CREATE OR REPLACE VIEW score.main AS

SELECT c.gbifID,
(0.3 * s.spatial_score + 0.15 * p.pheno_score + 0.25 * o.observer_score + 0.25 *  i.identifier_score + 3 * e.expert_id_score ) as score,
c.* EXCLUDE(c.'gbifID')

FROM features.combined_p c
JOIN score.spatial s ON c."gbifID" = s."gbifID"
JOIN score.phenology p ON c."gbifID" = p."gbifID"
JOIN score.observer o ON c."gbifID" = o."gbifID"
JOIN score.identifier i ON c."gbifID" = i."gbifID"
JOIN score.expert_id e ON c."gbifID" = e."gbifID"

