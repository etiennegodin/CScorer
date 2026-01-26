CREATE OR REPLACE VIEW score.main AS

SELECT c.gbifID,
(1 * s.spatial_score + 1 * p.pheno_score + 1 * o.observer_score + 1 *  i.identifier_score +  1 * e.cmva_expert_id ) as score,
c.* EXCLUDE(c.'gbifID')

FROM features.combined_p c
JOIN score.spatial s ON c."gbifID" = s."gbifID"
JOIN score.phenology p ON c."gbifID" = p."gbifID"
JOIN score.observer o ON c."gbifID" = o."gbifID"
JOIN score.identifier i ON c."gbifID" = i."gbifID"
JOIN encoded.community_validation e ON c."gbifID" = e."gbifID"

