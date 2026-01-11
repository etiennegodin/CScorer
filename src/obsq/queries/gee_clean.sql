CREATE OR REPLACE TABLE clean.gee AS
SELECT l."gbifID", g.* EXCLUDE( g."gee_id", g.'coordinates', g."gbifID")
FROM raw.gee_citizen_occurences g 
INNER JOIN labeled.gbif_citizen l
    ON l."gbifID" = g."gbifID";

INSERT INTO clean.gee
SELECT l."gbifID", g.* EXCLUDE( g."gee_id", g.'coordinates', g."gbifID")

FROM raw.gee_expert_occurences g 
INNER JOIN labeled.gbif_citizen l
    ON l."gbifID" = g."gbifID";