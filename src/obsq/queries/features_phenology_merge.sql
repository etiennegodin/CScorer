CREATE OR REPLACE VIEW features.phenology AS


SELECT pl.*,
pr.pheno_repro_month_density,
ps.pheno_sex_month_density

FROM features.phenology_leaves pl
JOIN features.phenology_repro pr ON pl."gbifID" = pr."gbifID"
JOIN features.phenology_sex ps ON pl."gbifID" = ps."gbifID"