CREATE OR REPLACE VIEW score.observer AS


SELECT g."gbifID",
( 0.2 * i.id_count ) + (0.2 * i.id_species_entropy) + (0.1 * i.obsv_avg_id_time) + (0.1 * i.id_avg_monthly_obs) +
(0.1 * i.id_avg_yearly_obs) +
(0.2 * oi.inat_id_log_z_score ) as observer_score



FROM preprocessed.gbif_citizen g
JOIN transformed.identifiers i ON g."identifiedBy" = i."identifiedBy"
JOIN transformed.observer_inat oi on g."identifiedBy" = oi."recordedBy"





