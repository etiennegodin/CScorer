CREATE INDEX IF NOT EXISTS idx_c_geom  ON preprocessed.gbif_citizen USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_e_geom  ON preprocessed.gbif_expert USING RTREE (geom);

CREATE OR REPLACE TABLE preprocessed.gbif_citizen_labeled AS

WITH c3857 AS(
    
    SELECT gbifID, "month", taxonKey, ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') AS geom
    FROM preprocessed.gbif_citizen
),

e3857 AS(
    
    SELECT gbifID, "month", taxonKey, ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') AS geom
    FROM preprocessed.gbif_expert
),

matched_ids AS(

SELECT e.gbifID as e_id, e.month as e_month, e.taxonKey as e_taxon,
    c.gbifID as c_id, c.month as c_month, c.taxonKey as c_taxon,

FROM e3857 e
JOIN c3857 c
    ON ST_DWithin(
        (SELECT geom FROM preprocessed.gbif_expert WHERE gbifID = e.gbifID),  -- index hit
        (SELECT geom FROM preprocessed.gbif_citizen WHERE gbifID = c.gbifID),  -- index hit
        0.1
    )
WHERE ST_DWithin(e.geom, c.geom, 5000) AND e_taxon = c_taxon

)

SELECT c.*, 
CASE
    WHEN c.gbifID IN (SELECT c_id FROM matched_ids)
    THEN 1
    ELSE 0
END AS expert_match,

FROM preprocessed.gbif_citizen c
;



