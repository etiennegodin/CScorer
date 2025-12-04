from .api import inatApiClient
from ...steps import *
from ...utils import gdf_to_duckdb
from ...pipeline import *


## OBSERVER SUBMODULE


get_observers_data = inatApiClient('inat_observers',
                                   endpoint= 'users/autocomplete/?q=',
                                   api_version=1,
                                   params_key= None,
                                   items_source= 'observers.citizen',
                                   items_key='id'
                                   per_page=1000,
                                   chunk_size=1,
                                   overwrite_table= True)

extract_observer_json = SimpleQuery('extract_observer_json', query_name= 'inat_extract_observer_json')
clean_observer = SimpleQuery('clean_observer', query_name= 'inat_clean_observers')

inat_observers_submodule = SubModule("inat_observers", [get_observers_data, extract_observer_json, clean_observer])
