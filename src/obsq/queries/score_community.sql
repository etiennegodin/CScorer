CREATE OR REPLACE VIEW score.community AS


SELECT g."gbifID",
1 / (1 + EXP(-1.3 * (o.obsv_unique_year_count - 2.5))) as longevity_score,
(LOG(1+o.obsv_obs_count) / LOG(10000)) as experience_score,
(1/50 * o.obsv_avg_monthly_obs) as consitency_score_month,
(1/100 * o.obsv_avg_yearly_obs) as consitency_score_month,
(LOG(1+o.obsv_avg_obs_time) / LOG(640)) as consitency_score_time,
(LOG(1+o.obsv_species_diversity) / LOG(7)) as species_diversity,
oi.inat_obv_log_z_score,
oi.inat_id_log_z_score,
oi.inat_sp_log_z_score


FROM preprocessed.gbif_citizen g
JOIN features.observer o ON g."recordedBy" = o."recordedBy"
JOIN features.observer_inat oi on g."recordedBy" = oi."recordedBy"
JOIN features.identifiers i on g."recordedBy" = i."identifiedBy"





