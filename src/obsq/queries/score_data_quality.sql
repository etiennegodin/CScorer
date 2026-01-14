CREATE OR REPLACE VIEW score.data_quality AS 

WITH multi_photo AS (

    SELECT "gbifID",
    COALESCE(CASE WHEN meta_media_count > 1  THEN 1 END, 0) AS has_multi_photo
    FROM features.metadata
),

gps_accuracy AS(

    SELECT "gbifID",
    COALESCE(CASE WHEN "meta_coordinateUncertaintyInMeters" < 300  THEN 1 END, 0) AS gps_accuracy
    FROM features.metadata

), remarks AS(

    SELECT "gbifID",
        COALESCE(CASE WHEN "meta_occurrenceRemarks" < 0   THEN 1 END, 0)AS has_remarks
    FROM features.metadata

)


SELECT m."gbifID",

(0.3 * mp.has_multi_photo  +
0.2 * g.gps_accuracy + 
0.15 * m.meta_pheno_repro +
0.15 * m.meta_pheno_leaves + 
0.1 * m.meta_pheno_sex + 
0.1 * r.has_remarks

) AS data_qualiy_score


FROM features.metadata m
JOIN multi_photo mp on m."gbifID" = mp."gbifID" 
JOIN gps_accuracy g on m."gbifID" = g."gbifID" 
JOIN remarks r on m."gbifID" = r."gbifID" 