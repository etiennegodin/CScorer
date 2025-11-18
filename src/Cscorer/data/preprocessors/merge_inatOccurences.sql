ALTER TABLE preprocessed.gbif_citizen ADD COLUMN url TEXT;
ALTER TABLE preprocessed.gbif_citizen ADD COLUMN image_url TEXT;
ALTER TABLE preprocessed.gbif_citizen ADD COLUMN num_identification_agreements INTEGER;
ALTER TABLE preprocessed.gbif_citizen ADD COLUMN num_identification_disagreements INTEGER;
ALTER TABLE preprocessed.gbif_citizen ADD COLUMN description INTEGER;


WITH inat_inter AS (

SELECT 
url, 
image_url,
num_identification_agreements,
num_identification_disagreements,
CASE
    WHEN description IS NOT NULL THEN 1
    ELSE 0
END AS description
FROM inat.occurences
)

UPDATE preprocessed.gbif_citizen g
SET 
    url = i.url,
    num_identification_agreements = i.num_identification_agreements,
    num_identification_disagreements = i.num_identification_disagreements,
    image_url = i.image_url,
    description = i.description,


FROM inat_inter i
WHERE g.occurrenceID = i.url;

DELETE FROM preprocessed.gbif_citizen g
WHERE g.url IS NULL;