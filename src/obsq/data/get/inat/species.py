from .api import inatApiClient, inatApiRequest
from ....db import DataBaseLoader, SimpleQuery
from ....utils import gdf_to_duckdb
from ....pipeline import *

## SPECIES SUBMODULE

create_species_table = SimpleQuery('create_species_table', query_name= 'inat_species_extract')

@step 
async def get_place_id(context:PipelineContext):
        
        return inatApiRequest('get_place_id',
                                endpoint= 'places/?q=',
                                key=context.config['place_id'],
                                limit=1,
                                api_version=2,
                                per_page=1)._execute(context)

#Phenology
@step
async def get_phenology_wrapper(context:PipelineContext):
        
        place_id = context.get_step_output("get_place_id")
        phenology_params = {'place_id': place_id['id'] }
        phenology_fields = {
        "count" : True,
        "controlled_attribute" : {"id":True, "label":True},
        "controlled_value" : {"id":True, "label":True},
        "month_of_year" : True
        }

        return inatApiClient('inat_phenology',
                                        endpoint= 'observations/popular_field_values',
                                        api_version=2,
                                        params_key= 'taxon_id',
                                        explicit_params=phenology_params,
                                        limiter = 50,
                                        items_source = 'preprocessed.species',
                                        items_key= 'taxonID',
                                        items_limit= None,
                                        fields= phenology_fields,
                                        per_page=10,
                                        chunk_size=1,
                                        overwrite_table= False)._execute(context)

extract_phenology_json = SimpleQuery('extract_phenology_json', query_name= 'inat_species_phenology_json')
pivot_phenology = SimpleQuery('pivot_phenology', query_name= 'inat_species_phenology_pivot')

#Similar species
@step 
async def similar_species_wrapper(context:PipelineContext):
        
        place_id = context.get_step_output("get_place_id")
        similar_species_params = {'place_id': place_id['id'] }

        return inatApiClient('inat_similar_species',
                                        endpoint= 'identifications/similar_species',
                                        api_version=2,
                                        params_key= 'taxon_id',
                                        explicit_params=similar_species_params,
                                        items_source = 'preprocessed.species',
                                        items_key= 'taxonID',
                                        items_limit= None,
                                        per_page=50,
                                        limiter = 50,
                                        chunk_size=1,
                                        overwrite_table= False)._execute(context)

extract_similar_species_json = SimpleQuery('extract_similar_species_json', query_name= 'inat_species_similar_json')

# Histogram
@step
async def get_histogram_wrapper(context:PipelineContext):
        
        place_id = context.get_step_output("get_place_id")
        histogram_params = {'place_id': place_id['id'] }
        histogram_fields = {
        "month_of_year" : True
        }
        return inatApiClient('inat_histogram',
                                        endpoint= 'observations/histogram',
                                        api_version=2,
                                        params_key= 'taxon_id',
                                        explicit_params=histogram_params,
                                        limiter = 50,
                                        items_source = 'preprocessed.species',
                                        items_key= 'taxonID',
                                        items_limit= None,
                                        per_page=2,
                                        chunk_size=1,
                                        fields= histogram_fields,
                                        overwrite_table= True)._execute(context)

extract_histogram_json = SimpleQuery('extract_histogram_json', query_name= 'inat_species_histogram_json')
pivot_histogram = SimpleQuery('pivot_histogram', query_name= 'inat_species_histogram_pivot')



inat_species_submodule = SubModule("inat_species", [create_species_table,
                                                    get_place_id,
                                                    get_phenology_wrapper,
                                                    extract_phenology_json,
                                                    pivot_phenology,
                                                    similar_species_wrapper,
                                                    extract_similar_species_json,
                                                    get_histogram_wrapper,
                                                    extract_histogram_json,
                                                    pivot_histogram])