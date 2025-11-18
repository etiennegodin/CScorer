CREATE INDEX idx_c_geom ON preprocessed.gbif_citizen USING RTREE (geom_c);
CREATE INDEX idx_e_geom ON preprocessed.gbif_expert USING RTREE (geom_e);

WITH c3857 AS(
    
    SELECT gbifID, ST_Transform(geom_c, 3857) AS geom
    FROM preprocessed.gbif_citizen
),

WITH e3857 AS(
    
    SELECT gbifID, ST_Transform(geom_e, 3857) AS geom
    FROM preprocessed.gbif_expert
)

CREATE OR REPLACE TABLE preprocessed.matchedGbif AS
SELECT e.gbifID as e_id,
    c.gbifID as c_id,

FROM e3857 e
JOIN c3857 c
    ON ST_DWithin(
        (SELECT geom_e FROM preprocessed.gbif_expert WHERE gbifID = e.gbifID),  -- index hit
        (SELECT geom_c FROM preprocessed.gbif_citizen WHERE gbifID = c.gbifID),  -- index hit
        0.1
    )
WHERE ST_DWithin(e.geom, c.geom, 5000);   -- accurate metric distance