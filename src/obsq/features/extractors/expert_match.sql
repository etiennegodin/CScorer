SUM(expert_match) AS expert_match_count,
ROUND(100 * (SUM(expert_match) / observations_count),2) as expert_match_pct,
ROUND(100.0000 * (SUM(expert_match) / SUM(COUNT(*)) OVER ()), 3) AS expert_match_total_pct,