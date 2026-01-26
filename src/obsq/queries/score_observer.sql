CREATE OR REPLACE VIEW score.observer AS


SELECT g."gbifID",
COALESCE(( 0.2 * o.obsv_species_diversity ) + (0.2 * o.obsv_obs_count) + (0.1 * o.obs_per_year) + (0.05 * o.obsv_sex_meta_pct) +
(0.05 * o.obsv_repro_cond_meta_pct) + (0.05 * o.obsv_annot_meta_pct) + (0.05 * o.obsv_avg_obs_time) + 
(0.1 * oi.inat_obv_log_z_score) +
(0.1 * oi.inat_sp_log_z_score ),0) as observer_score

FROM preprocessed.gbif_citizen g
JOIN transformed.observer o on g.recordedBy = o.recordedBy
JOIN transformed.observer_inat oi on g."recordedBy" = oi."recordedBy"





