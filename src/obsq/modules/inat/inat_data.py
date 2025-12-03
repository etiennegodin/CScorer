from .inat_api import inatApiClient
from ...steps import DataBaseLoader, DataBaseQuery
from ...pipeline import PipelineContext,Module, SubModule, step

## OBSERVER SUBMODULE

get_observer_list = DataBaseLoader('get_observer_list', columns='recordedBy',from_table='raw.citizen_observers', limit = 50, return_type= 'list')

get_observers_data = inatApiClient('inat_observers',
                                   endpoint= 'users/autocomplete/?q=',
                                   api_version=1,
                                   params_key= None,
                                   items_from = 'get_observer_list',
                                   per_page=1000,
                                   chunk_size=1,
                                   overwrite_table= True)

extract_observer_json = DataBaseQuery('extract_observer_json', query_name= 'inat_extract_observer_json')
clean_observer = DataBaseQuery('clean_observer', query_name= 'inat_clean_observers')

inat_observers_submodule = SubModule("inat_observers", [get_observer_list, get_observers_data, extract_observer_json, clean_observer])

clean_gbif_post_inat = DataBaseQuery('clean_gbif_post_inat', query_name= 'gbif_clean_post_inat')


## SPECIES SUBMODULE


get_species_taxon_list = DataBaseLoader('get_species_taxon_list', columns='taxonID',from_table='preprocessed.gbif_citizen', limit = 10, return_type= 'list')

phenology_fields = {
        "count" : True,
        "controlled_attribute" : {"id":True, "label":True},
        "controlled_value" : {"id":True, "label":True},
        "month_of_year" : True

}

get_phenology_data = inatApiClient('inat_phenology',
                                   endpoint= 'observations/popular_field_values',
                                   api_version=2,
                                   params_key= 'taxon_id',
                                   items_from = 'get_species_taxon_list',
                                   fields= phenology_fields,
                                   per_page=1000,
                                   chunk_size=1,
                                   overwrite_table= True)

extract_phenology_json = DataBaseQuery('extract_phenology_json', query_name= 'inat_extract_phenology_json')

get_similar_species_data = inatApiClient('inat_similar_species',
                                   endpoint= 'identifications/similar_species',
                                   api_version=2,
                                   params_key= 'taxon_id',
                                   items_from = 'get_species_taxon_list',
                                   per_page=1000,
                                   chunk_size=1,
                                   overwrite_table= True)

extract_phenology_json = DataBaseQuery('extract_phenology_json', query_name= 'inat_extract_similar_species_json')

#inat_species_submodule = SubModule("inat_species", [get_species_taxon_list, get_phenology_data, extract_phenology_json, get_similar_species_data, extract_phenology_json])
inat_species_submodule = SubModule("inat_species", [get_species_taxon_list, get_similar_species_data, extract_phenology_json])

inat_data_module = Module('inat_data',[inat_observers_submodule, clean_gbif_post_inat, inat_species_submodule], always_run= False )
