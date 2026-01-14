CREATE OR REPLACE VIEW score.main AS

SELECT c.gbifID,

(0.40 * s.spatial_match + 0.35 * e.expert_match_score + 0.05 * cv.cmva_id_agree_rate +
 0.05 * pl.pheno_leaves_month_density + 0.05 * pr.pheno_repro_month_density + 0.05 * ps.pheno_sex_month_density +
 0.05 * h.histo_taxon_obs_month_density +
 0.10 * r.range 
 
) AS score


FROM preprocessed.gbif_citizen c
JOIN score.spatial_match s ON c."gbifID" = s."gbifID"
JOIN score.expert_match e ON c."gbifID" = e."gbifID"
JOIN features.community_validation cv ON c."gbifID" = cv."gbifID"
JOIN features.phenology_leaves pl on c."gbifID" = pl."gbifID"
JOIN features.phenology_repro pr on c."gbifID" = pr."gbifID"
JOIN features.phenology_sex ps on c."gbifID" = ps."gbifID"
JOIN features.histogram h on c."gbifID" = h."gbifID"
JOIN features.ranges r on c."gbifID" = r."gbifID"
JOIN features.metadata m on c."gbifID" = m."gbifID"