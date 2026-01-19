CREATE OR REPLACE TABLE score.phenology AS 


SELECT p.gbifID,
(0.25 * h.histo_taxon_obs_month_density) +
(0.25 * p.pheno_leaves_month_density) + 
(0.25 * p.pheno_repro_month_density) + 
(0.25 * p.pheno_sex_month_density) as pheno_score

FROM encoded.phenology p
JOIN encoded.histogram h on h."gbifID" = p."gbifID"