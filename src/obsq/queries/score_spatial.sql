
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
SELECT c.gbifID,
ST_Distance(e.geom, c.geom) as dist,
1 / (1 + EXP(0.5 * (ST_Distance(e.geom, c.geom) - 10))) AS spatial_score

FROM e3857 e
JOIN c3857 c
    ON ST_DWithin(
        (SELECT geom FROM preprocessed.gbif_expert WHERE gbifID = e.gbifID),  -- spatial index hit
        (SELECT geom FROM preprocessed.gbif_citizen WHERE gbifID = c.gbifID),  -- spatial index hit
        .1
    )
WHERE e.species = c.species --same species  




