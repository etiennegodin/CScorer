CREATE OR REPLACE TABLE features.combined AS

SELECT g.gbifID,
    g.expert_match,
    g.species,
    o.* EXCLUDE(o."recordedBy"),
    h.* EXCLUDE(h."gbifID"),
    c.* EXCLUDE(c."gbifID"),
    a.* EXCLUDE(a."gbifID"),
    m.* EXCLUDE(m."gbifID"),
    t.* EXCLUDE(t."taxonID")

FROM labeled.gbif_citizen g
LEFT JOIN features.observer o
    ON o."recordedBy" = g."recordedBy"
LEFT JOIN features.taxonomic t
    ON t."taxonID" = g."taxonID"
JOIN features.community_validation c
    ON c."gbifID" = g."gbifID"
JOIN features.metadata m
    ON m."gbifID" = g."gbifID"
JOIN features.histogram h
    ON h."gbifID" = g."gbifID"
JOIN features.temporal a
    ON a."gbifID" = g."gbifID";

    
    ;




