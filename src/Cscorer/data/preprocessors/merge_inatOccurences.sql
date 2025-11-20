ALTER TABLE preprocessed.gbif_citizen ADD COLUMN IF NOT EXISTS url TEXT;
ALTER TABLE preprocessed.gbif_citizen ADD COLUMN IF NOT EXISTS image_url TEXT;
ALTER TABLE preprocessed.gbif_citizen ADD COLUMN IF NOT EXISTS num_identification_agreements INTEGER;
ALTER TABLE preprocessed.gbif_citizen ADD COLUMN IF NOT EXISTS num_identification_disagreements INTEGER;
ALTER TABLE preprocessed.gbif_citizen ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE preprocessed.gbif_citizen ADD COLUMN IF NOT EXISTS description_length INT;


WITH inat_inter AS (

SELECT 
observed_on,
url, 
image_url,
num_identification_agreements,
num_identification_disagreements,
"description",
LEN("description") as description_length
FROM inat.occurences
)

UPDATE preprocessed.gbif_citizen g
SET 
    url = i.url,
    num_identification_agreements = i.num_identification_agreements,
    num_identification_disagreements = i.num_identification_disagreements,
    image_url = i.image_url,
    description = i.description,
    description_length = i.description_length



FROM inat_inter i
WHERE g.occurrenceID = i.url;

DELETE FROM preprocessed.gbif_citizen g
WHERE g.url IS NULL;