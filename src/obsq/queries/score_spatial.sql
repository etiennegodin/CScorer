
-- create spatial index for spatial join
CREATE INDEX IF NOT EXISTS idx_c_geom  ON preprocessed.gbif_citizen USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_e_geom  ON preprocessed.gbif_expert USING RTREE (geom);

CREATE OR REPLACE TABLE score.spatial_match AS

--projected citizen data 
WITH c3857 AS(
    
    SELECT gbifID, "month", species, ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') AS geom
    FROM preprocessed.gbif_citizen
),
--projected expert data 
e3857 AS(
    
    SELECT gbifID, "month", species, ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') AS geom
    FROM preprocessed.gbif_expert
)

-- main label rules 
SELECT e.gbifID as e_id, e.species as e_taxon,
    c.gbifID as c_id, c.species as c_taxon,

CASE 
    WHEN ST_Distance(e.geom, c.geom) <= 1000 THEN 1.0
    WHEN ST_Distance(e.geom, c.geom) <= 5000 THEN 0.66
    WHEN ST_Distance(e.geom, c.geom) <= 10000 THEN 0.33
    ELSE 0.0
END AS spatial_match

FROM e3857 e
JOIN c3857 c
    ON ST_DWithin(
        (SELECT geom FROM preprocessed.gbif_expert WHERE gbifID = e.gbifID),  -- spatial index hit
        (SELECT geom FROM preprocessed.gbif_citizen WHERE gbifID = c.gbifID),  -- spatial index hit
        0.1
    )
WHERE e_taxon = c_taxon --same species  




