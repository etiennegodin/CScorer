from .api import inatApiClient, inatApiRequest
from ...steps import DataBaseLoader, SimpleQuery
from ...utils import gdf_to_duckdb
from ...pipeline import *

## SPECIES SUBMODULE

create_species_table = SimpleQuery('create_species_table', query_name= 'gbif_extract_species')

@step 
async def get_place_id(context:PipelineContext):
        
        return inatApiRequest('place_id',
                                endpoint= 'places/?q=',
                                key=context.config['place_id'],
                                limit=1,
                                api_version=2,
                                per_page=1)._execute(context)

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

        await inatApiClient('inat_phenology',
                                        endpoint= 'observations/popular_field_values',
                                        api_version=2,
                                        params_key= 'taxon_id',
                                        explicit_params=phenology_params,
                                        limiter = 20,
                                        items_source = 'preprocessed.species',
                                        items_key= 'taxonID',
                                        fields= phenology_fields,
                                        per_page=10,
                                        chunk_size=1,
                                        overwrite_table= True)._execute(context)

extract_phenology_json = SimpleQuery('extract_phenology_json', query_name= 'inat_extract_phenology_json')

get_similar_species_data = inatApiClient('inat_similar_species',
                                   endpoint= 'identifications/similar_species',
                                   api_version=2,
                                   params_key= 'taxon_id',
                                   items_source = 'preprocessed.species',
                                   items_key= 'taxonID',
                                   per_page=50,
                                   limiter = 50,
                                   chunk_size=1,
                                   overwrite_table= True)

extract_similar_species_json = SimpleQuery('extract_similar_species_json', query_name= 'inat_extract_similar_species_json')

"""
inat_species_submodule = SubModule("inat_species", [create_species_table,
                                                    get_place_id,
                                                    get_phenology_wrapper,
                                                    extract_phenology_json,
                                                    get_similar_species_data,
                                                    extract_similar_species_json])

"""
inat_species_submodule = SubModule("inat_species", [create_species_table,
                                                    get_place_id,
                                                    get_phenology_wrapper,
                                                    extract_phenology_json])