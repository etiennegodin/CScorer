
-- create spatial index for spatial join
CREATE INDEX IF NOT EXISTS idx_c_geom  ON preprocessed.gbif_citizen USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_e_geom  ON preprocessed.gbif_expert USING RTREE (geom);

CREATE OR REPLACE TABLE labeled.gbif_citizen AS

WITH c3857 AS(
    
    SELECT gbifID, "month", taxonID, ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') AS geom
    FROM preprocessed.gbif_citizen
),

e3857 AS(
    
    SELECT gbifID, "month", taxonID, ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') AS geom
    FROM preprocessed.gbif_expert
),
-- main label rules 
matched_ids AS( 

SELECT e.gbifID as e_id, e.month as e_month, e.taxonID as e_taxon,
    c.gbifID as c_id, c.month as c_month, c.taxonID as c_taxon,

FROM e3857 e
JOIN c3857 c
    ON ST_DWithin(
        (SELECT geom FROM preprocessed.gbif_expert WHERE gbifID = e.gbifID),  -- spatial index hit
        (SELECT geom FROM preprocessed.gbif_citizen WHERE gbifID = c.gbifID),  -- spatial index hit
        0.1
    )
WHERE ST_DWithin(e.geom, c.geom, 3000) --accurate distance measure
AND e_taxon = c_taxon --same species  

)

SELECT c.*, 
CASE -- label 0 or 1 
    WHEN c.gbifID IN (SELECT c_id FROM matched_ids)
    THEN 1
    ELSE 0
END AS expert_match,

FROM preprocessed.gbif_citizen c
;



