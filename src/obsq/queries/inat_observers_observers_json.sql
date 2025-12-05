CREATE OR REPLACE VIEW raw.inat_observers_extracted AS


SELECT 
item_key as user_id,

CAST(json_extract(json, '$.id')AS INTEGER) AS id,
json_extract_string(json, '$.login') AS login,
json_extract(json, '$.spam') AS spam,
json_extract(json, '$.suspended') AS suspended,
json_extract(json, '$.created_at') AS created_at,
json_extract_string(json, '$.login_autocomplete') AS login_autocomplete,
json_extract_string(json, '$.login_exact') AS login_exact,
json_extract_string(json, '$.name') AS "name",
json_extract_string(json, '$.name_autocomplete') AS name_autocomplete,
json_extract_string(json, '$.orcid') AS orcid,
json_extract(json, '$.icon') AS icon,
CAST(json_extract(json, '$.observations_count')AS INTEGER) AS observations_count,
CAST(json_extract(json, '$.identifications_count')AS INTEGER) AS identifications_count,
json_extract(json, '$.journal_posts_count') AS journal_posts_count,
json_extract(json, '$.activity_count') AS activity_count,
CAST(json_extract(json, '$.species_count')AS INTEGER) AS species_count,
CAST(json_extract(json, '$.universal_search_rank')AS INTEGER) AS universal_search_rank,
json_extract_string(json, '$.roles') AS roles,
json_extract(json, '$.site_id') AS site_id,
json_extract_string(json, '$.icon_url') AS icon_url

FROM raw.inat_observers;
