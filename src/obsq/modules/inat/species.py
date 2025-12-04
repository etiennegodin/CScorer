from .api import inatApiClient
from ...steps import DataBaseLoader, SimpleQuery
from ...utils import gdf_to_duckdb
from ...pipeline import *

## SPECIES SUBMODULE

get_species_taxon_list = DataBaseLoader('get_species_taxon_list', columns='taxonID',from_table='clean.species', limit = None, return_type= 'list')

phenology_fields = {
        "count" : True,
        "controlled_attribute" : {"id":True, "label":True},
        "controlled_value" : {"id":True, "label":True},
        "month_of_year" : True

}
"""
get_phenology_data = inatApiClient('inat_phenology',
                                   endpoint= 'observations/popular_field_values',
                                   api_version=2,
                                   params_key= 'taxon_id',
                                   limiter = 20,
                                   items_from = 'get_species_taxon_list',
                                   fields= phenology_fields,
                                   per_page=10,
                                   chunk_size=1,
                                   overwrite_table= True)

extract_phenology_json = SimpleQuery('extract_phenology_json', query_name= 'inat_extract_phenology_json')

get_similar_species_data = inatApiClient('inat_similar_species',
                                   endpoint= 'identifications/similar_species',
                                   api_version=2,
                                   params_key= 'taxon_id',
                                   items_from = 'get_species_taxon_list',
                                   per_page=1000,
                                   limiter = 50,
                                   chunk_size=1,
                                   overwrite_table= True)
"""
extract_similar_species_json = SimpleQuery('extract_similar_species_json', query_name= 'inat_extract_similar_species_json')

inat_species_submodule = SubModule("inat_species", [get_species_taxon_list])
#inat_species_submodule = SubModule("inat_species", [get_species_taxon_list, get_phenology_data, extract_phenology_json, get_similar_species_data, extract_similar_species_json])
