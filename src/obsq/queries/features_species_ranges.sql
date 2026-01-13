
-- create spatial index for spatial join
CREATE INDEX IF NOT EXISTS idx_geom  ON preprocessed.gbif_citizen USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_geom  ON raw.species_range USING RTREE (geom);

CREATE OR REPLACE TABLE features.ranges AS

WITH reprojected_citizen AS(
    
    SELECT gbifID, "taxonID", ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') AS geom
    FROM preprocessed.gbif_citizen
),

reprojected_ranges AS(
    
    SELECT taxon_id AS "taxonID", ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') AS geom
    FROM raw.species_range
)

SELECT c.gbifID,
CASE 
    WHEN ST_Within(c.geom, r.geom) THEN  1
    ELSE 0
END AS "range"

FROM reprojected_citizen c
JOIN reprojected_ranges r ON r."taxonID" = c."taxonID"




