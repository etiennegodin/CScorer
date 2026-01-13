SELECT c.gbifID,

(0.3 * s.spatial_match + 0.25 * e.expert_match_score + 0.1 * cv.cmva_id_agree_rate +
 0.05 * pl.pheno_leaves_month_density + 0.05 pr.pheno_repro_month_density + 0.05 * pheno_sex_month_density +
 



) AS score


FROM preprocessed.gbif_citizen c
JOIN score.spatial_match s ON c."gbifID" = s."gbifID"
JOIN score.expert_match e ON c."gbifID" = e."gbifID"
JOIN features.community_validation cv ON c."gbifID" = cv."gbifID"
JOIN features.phenology_leaves pl on c."gbifID" = pl."gbifID"
JOIN features.phenology_repro pr on c."gbifID" = pr."gbifID"
JOIN features.phenology_sex ps on c."gbifID" = ps."gbifID"