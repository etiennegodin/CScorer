CREATE OR REPLACE VIEW score.main AS

SELECT g.gbifID,

s.spatial_match,
e.expert_match_score,
c.community_score,
d.data_qualiy_score,
sc.scientific_score




FROM preprocessed.gbif_citizen g
JOIN score.spatial_match s ON g."gbifID" = s."gbifID"
JOIN score.expert_match e ON g."gbifID" = e."gbifID"
JOIN score.community c ON g."gbifID" = c."gbifID"
JOIN score.data_quality d ON g."gbifID" = d."gbifID"
JOIN score.scientific sc ON g."gbifID" = sc."gbifID"
