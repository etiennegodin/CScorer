CREATE OR REPLACE VIEW clean.species AS 

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

FROM preprocessed.gbif_citizen

