from .api import inatApiClient
from ....db import *
from ....utils import gdf_to_duckdb
from ....pipeline import *

## OBSERVER SUBMODULE
get_observers_data = inatApiClient('get_inat_observers_data',
                                   endpoint= 'users/',
                                   api_version=1,
                                   params_key= None,
                                   items_source= 'observers.citizen',
                                   items_key='user_id',
                                   limiter=40,
                                   per_page=1,
                                   overwrite_table= False)

extract_observer_json = SqlQuery('extract_observer_json', query_name= 'inat_observers_observers_json')
clean_observer = SqlQuery('clean_observer', query_name= 'inat_clean_observers')

inat_observers_module = Module("inat_observers", [get_observers_data, extract_observer_json, clean_observer])
