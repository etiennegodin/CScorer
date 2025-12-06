CREATE OR REPLACE TABLE features.combined AS

SELECT g.gbifID,
    g.expert_match,
    o.* EXCLUDE(o."recordedBy"),
    h.* EXCLUDE(h."gbifID"),
    c.* EXCLUDE(c."gbifID"),
    p.* EXCLUDE(p."gbifID"),
    t.* EXCLUDE(t."taxonID")
FROM labeled.gbif_citizen g
JOIN features.observer o
    ON o."recordedBy" = g."recordedBy"
JOIN features.community_validation c
    ON c."gbifID" = g."gbifID"
JOIN features.histogram h
    ON h."gbifID" = g."gbifID"
JOIN features.phenology p
    ON p."gbifID" = g."gbifID"
JOIN features.taxonomic t
    ON t."taxonID" = g."taxonID";




