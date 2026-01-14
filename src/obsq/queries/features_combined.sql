CREATE OR REPLACE TABLE features.combined AS

SELECT g.gbifID,
    s.* EXCLUDE (s."gbifID"),
    o.* EXCLUDE(o."recordedBy"),
    h.* EXCLUDE(h."gbifID"),
    c.* EXCLUDE(c."gbifID"),
    a.* EXCLUDE(a."gbifID"),
    m.* EXCLUDE(m."gbifID"),
    p.* EXCLUDE(p."gbifID"),
    t.* EXCLUDE(t."taxonID"),
    gee.* EXCLUDE(gee."gbifID"),
    sp.spatial_cluster,
    r.range

FROM preprocessed.gbif_citizen g
INNER JOIN score.main s
    on g."gbifID" = s."gbifID"
INNER JOIN encoded.observer o
    ON o."recordedBy" = g."recordedBy"
INNER JOIN encoded.taxonomic t
    ON t."taxonID" = g."taxonID"
INNER JOIN encoded.community_validation c
    ON c."gbifID" = g."gbifID"
INNER JOIN encoded.metadata m
    ON m."gbifID" = g."gbifID"
INNER JOIN encoded.histogram h
    ON h."gbifID" = g."gbifID"
INNER JOIN encoded.temporal a
    ON a."gbifID" = g."gbifID"
LEFT JOIN encoded.phenology p
    on g."gbifID" = p."gbifID"
INNER JOIN encoded.spatial sp
    on g."gbifID" = sp."gbifID"
INNER JOIN encoded.gee gee
    on g."gbifID" = gee."gbifID"
INNER JOIN features.ranges r
    on g."gbifID" = r."gbifID"
    ;




