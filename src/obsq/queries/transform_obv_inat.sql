CREATE OR REPLACE TABLE transformed.observer_inat AS

WITH stats AS (
    SELECT
        AVG(LOG(1 + observations_count)) AS obv_mean_log,
        STDDEV_POP(LOG(1 + observations_count)) AS obv_std_log,

        AVG(LOG(1 + identifications_count)) AS id_mean_log,
        STDDEV_POP(LOG(1 + identifications_count)) AS id_std_log,
        
        AVG(LOG(1 + species_count)) AS sp_mean_log,
        STDDEV_POP(LOG(1 + species_count)) AS sp_std_log,

        AVG(LOG(1 + universal_search_rank)) AS rank_mean_log,
        STDDEV_POP(LOG(1 + universal_search_rank)) AS rank_std_log,

    FROM observers.inat_data 
)

SELECT
    c."recordedBy",
    (LOG(1 + i.observations_count) - s.obv_mean_log)
        / NULLIF(s.obv_std_log, 0) AS inat_obv_log_z_score,

    (LOG(1 + i.identifications_count) - s.id_mean_log)
        / NULLIF(s.id_std_log, 0) AS inat_id_log_z_score,

    (LOG(1 + i.species_count) - s.sp_mean_log)
        / NULLIF(s.sp_std_log, 0) AS inat_sp_log_z_score,

    (LOG(1 + i.universal_search_rank) - s.rank_mean_log)
        / NULLIF(s.rank_std_log, 0) AS inat_rank_log_z_score,


    ROUND(PERCENT_RANK() OVER (ORDER BY i.observations_count),3) AS obs_p_rank,
    ROUND(PERCENT_RANK() OVER (ORDER BY i.observations_count),3) AS id_p_rank,
    ROUND(PERCENT_RANK() OVER (ORDER BY i.observations_count),3) AS sp_p_rank,
    ROUND(PERCENT_RANK() OVER (ORDER BY i.observations_count),3) AS rank_p_rank
FROM observers.inat_data i
JOIN observers.citizen c on i.user_id = c.user_id
CROSS JOIN stats s;
