CREATE OR REPLACE VIEW preprocessed.species AS 

SELECT DISTINCT
taxonID,
species,
taxonRank,
phylum,
"class",
"order",
family,
genus,
iucnRedListCategory

FROM preprocessed.gbif_post_inat_observers

