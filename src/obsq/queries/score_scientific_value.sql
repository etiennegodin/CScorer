CREATE OR REPLACE VIEW score.scientific AS

WITH species_rarity AS(

    SELECT "taxonID",
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER()* 10 ,5) AS rarity



    FROM preprocessed.gbif_citizen
    GROUP BY "taxonID"

)

SELECT g.gbifID,
r.rarity AS scientific_score
FROM preprocessed.gbif_citizen g
JOIN species_rarity r ON g."taxonID" = r."taxonID"

