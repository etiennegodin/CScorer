SELECT 
COUNT(*) as obs_count,
SUM(expert_match),
(SUM(expert_match) / obs_count) as match_pct

FROM preprocessed.gbif_citizen