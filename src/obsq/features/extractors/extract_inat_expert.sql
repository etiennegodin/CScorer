CREATE OR REPLACE VIEW preprocessed.inat_expert AS

SELECT recordedBy

FROM features.all_observers
WHERE orcid IS NOT NULL;