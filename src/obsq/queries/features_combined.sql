CREATE OR REPLACE TABLE features.combined AS

SELECT g.gbifID,
    g.expert_match,
    g.species,
    o.* EXCLUDE(o."recordedBy"),
    h.* EXCLUDE(h."gbifID"),
    c.* EXCLUDE(c."gbifID"),
    a.* EXCLUDE(a."gbifID"),
    m.* EXCLUDE(m."gbifID"),
    l.* EXCLUDE(l."gbifID"),
    s.* EXCLUDE(s."gbifID"),
    r.* EXCLUDE(r."gbifID"),
    t.* EXCLUDE(t."taxonID"),
    gee.* EXCLUDE(gee."gbifID"),
    sp.spatial_cluster


FROM labeled.gbif_citizen g

INNER JOIN features.observer o
    ON o."recordedBy" = g."recordedBy"
INNER JOIN features.taxonomic t
    ON t."taxonID" = g."taxonID"
INNER JOIN features.community_validation c
    ON c."gbifID" = g."gbifID"
INNER JOIN features.metadata m
    ON m."gbifID" = g."gbifID"
INNER JOIN features.histogram h
    ON h."gbifID" = g."gbifID"
INNER JOIN features.temporal a
    ON a."gbifID" = g."gbifID"
LEFT JOIN features.phenology_leaves l
    on g."gbifID" = l."gbifID"
LEFT JOIN features.phenology_sex s
    on g."gbifID" = s."gbifID"
LEFT JOIN features.phenology_repro r
    on g."gbifID" = r."gbifID"
INNER JOIN features.spatial sp
    on g."gbifID" = sp."gbifID"
INNER JOIN clean.gee gee
    on g."gbifID" = gee."gbifID"

    ;




