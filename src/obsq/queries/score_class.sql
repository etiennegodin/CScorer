-- create spatial index for spatial join
CREATE INDEX IF NOT EXISTS idx_c_geom  ON preprocessed.gbif_citizen USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_e_geom  ON preprocessed.gbif_expert USING RTREE (geom);

CREATE OR REPLACE TABLE score.class AS

-----spatial data handling first -----
--projected citizen data 
WITH c3857 AS(
    
    SELECT gbifID, "month", species, ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') AS geom
    FROM preprocessed.gbif_citizen
),
--projected expert data 
e3857 AS(
    
    SELECT gbifID, "month", species, ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') AS geom
    FROM preprocessed.gbif_expert
), 
--distance to observation 
distance AS(

SELECT c.gbifID, e.month as "month",
ST_Distance(e.geom, c.geom) as distance 
FROM e3857 e
JOIN c3857 c
    ON ST_DWithin(
        (SELECT geom FROM preprocessed.gbif_expert WHERE gbifID = e.gbifID),  -- spatial index hit
        (SELECT geom FROM preprocessed.gbif_citizen WHERE gbifID = c.gbifID),  -- spatial index hit
        0.1
    )
WHERE e.species = c.species

),

--user data -- 

observers AS (

    SELECT i.observations_count,
    i.identifications_count,
    c."recordedBy" as "recordedBy"
    
    FROM observers.inat_data i
    JOIN observers.citizen c ON i.user_id = c.user_id

), 
class5 AS(

SELECT g."gbifID",
5 AS class

FROM preprocessed.gbif_citizen g
JOIN distance d ON g."gbifID" = d."gbifID"
JOIN features.community_validation cv ON g.gbifID = cv.gbifID
JOIN features.metadata m ON g.gbifID = m.gbifID

WHERE d.month = g.month 
AND d.distance < 2000 
AND cv.cmva_expert_id =1 
AND m.meta_media_count >=2 
AND m."meta_coordinateUncertaintyInMeters" < 300
), class4 AS(

SELECT g."gbifID", 
4 AS class

FROM preprocessed.gbif_citizen g
JOIN distance d ON g."gbifID" = d."gbifID"
JOIN features.community_validation cv ON g.gbifID = cv.gbifID
JOIN features.metadata m ON g.gbifID = m.gbifID
JOIN features.phenology p ON g.gbifID = p.gbifID
JOIN features.histogram h ON g.gbifID = h.gbifID
JOIN features.ranges r ON g.gbifID = r.gbifID
JOIN features.observers_taxon_id o ON g."identifiedBy" = g."identifiedBy"

WHERE d.distance < 5000 
AND h.histo_taxon_obs_month_density > 0.5 
AND (cv.cmva_expert_id = 1 OR (cv.cmva_id_agree_rate = 1 AND cv.cmva_id_count >=3))
AND m.meta_media_count >=2 
), class3 AS(

SELECT g."gbifID",
3 AS class

FROM preprocessed.gbif_citizen g
JOIN features.community_validation cv ON g.gbifID = cv.gbifID
JOIN features.ranges r ON g.gbifID = r.gbifID
JOIN features.observers_taxon_id o ON g."identifiedBy" = g."identifiedBy"

WHERE r.range = 1
AND o.taxon_id_count > 5 
AND cv.cmva_id_count >= 3 

),class2 AS(

SELECT g."gbifID",
2 AS class

FROM preprocessed.gbif_citizen g
JOIN features.community_validation cv ON g.gbifID = cv.gbifID
JOIN features.histogram h ON g.gbifID = h.gbifID
JOIN features.ranges r ON g.gbifID = r.gbifID

WHERE r.range =1 
AND cv.cmva_id_agree_rate < 1.0 
AND h.histo_taxon_obs_month_density < 0.5

    
), class1 AS(

SELECT g."gbifID",
1 AS class

FROM preprocessed.gbif_citizen g
JOIN features.ranges r ON g.gbifID = r.gbifID
WHERE r.range = 1 
)


SELECT g.gbifID,
COALESCE (c5.class, c4.class,c3.class,c2.class,c1.class, -1) AS class

FROM preprocessed.gbif_citizen g
LEFT JOIN class5 c5 ON g."gbifID" = c5."gbifID"
LEFT JOIN class4 c4 ON g."gbifID" = c4."gbifID"
LEFT JOIN class3 c3 ON g."gbifID" = c3."gbifID"
LEFT JOIN class2 c2 ON g."gbifID" = c2."gbifID"
LEFT JOIN class1 c1 ON g."gbifID" = c1."gbifID"


