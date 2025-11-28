CREATE OR REPLACE VIEW clean.species AS 

SELECT 
taxonID,
scientificName,
taxonRank,
phylum,
"class",
"order",
family,
genus,
iucnRedListCategory

FROM clean.gbif_citizen

