CREATE OR REPLACE VIEW score.observer AS


WITH experience AS(
    SELECT o."recordedBy",
    LOG(1+o.obsv_obs_count) / 10 as epxerience
    FROM transformed.observer o 
),

species_specialist AS(

    SELECT g."recordedBy",
    COALESCE(t.species_prob,0) as species_prob
    FROM preprocessed.gbif_citizen
    JOIN transformed.observers_taxon_obsv t ON g."recordedBy" = t."recordedBy"
    WHERE g."taxonID" = t."taxonID"
)

SELECT g."gbifID",
0.3 * e.experience + 
0.2 * o.obsv_species_diversity + 
0.2 * o.obsv_unique_year_count +
0.2 * s.species_prob 


FROM preprocessed.gbif_citizen g
JOIN transformed.observer o on g.recordedBy = o.recordedBy
JOIN experience e ON g."recordedBy" = e."recordedBy"
JOIN species_specialist s on g."recordedBy" = s.recordedBy




