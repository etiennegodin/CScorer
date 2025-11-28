CREATE OR REPLACE VIEW clean.species AS 

SELECT DISTINCT
taxonID,
scientificName,
taxonRank,
phylum,
"class",
"order",
family,
genus,
iucnRedListCategory

FROM preprocessed.gbif_citizen

