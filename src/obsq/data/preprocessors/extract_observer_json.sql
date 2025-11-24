  CREATE OR REPLACE VIEW preprocessed.inat_observers_prep AS
  SELECT 
  json_extract(json, '$.id') AS id,
  json_extract(json, '$.login') AS login,
  json_extract(json, '$.spam') AS spam,
  json_extract(json, '$.suspended') AS suspended,
  json_extract(json, '$.created_at') AS created_at,
  json_extract(json, '$.login_autocomplete') AS login_autocomplete,
  json_extract(json, '$.login_exact') AS login_exact,
  json_extract(json, '$.name') AS name,
  json_extract(json, '$.name_autocomplete') AS name_autocomplete,
  json_extract(json, '$.orcid') AS orcid,
  json_extract(json, '$.icon') AS icon,
  json_extract(json, '$.observations_count') AS observations_count,
  json_extract(json, '$.identifications_count') AS identifications_count,
  json_extract(json, '$.journal_posts_count') AS journal_posts_count,
  json_extract(json, '$.activity_count') AS activity_count,
  json_extract(json, '$.species_count') AS species_count,
  json_extract(json, '$.universal_search_rank') AS universal_search_rank,
  json_extract(json, '$.roles') AS roles,
  json_extract(json, '$.site_id') AS site_id,
  json_extract(json, '$.icon_url') AS icon_url

FROM raw.inat_observers;

-- remove duplicates
CREATE OR REPLACE TABLE preprocessed.inat_observers AS
SELECT * FROM preprocessed.inat_observers_prep
WHERE id NOT IN (
  SELECT MIN(id)
  FROM preprocessed.inat_observers 
);