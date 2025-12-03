from .inat_api import inatApiClient
from ...steps import DataBaseLoader, DataBaseQuery
from ...pipeline import PipelineContext,Module, SubModule, step


get_observer_list_query = """SELECT * FROM """

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

inat_observers = SubModule("inat_observers", [get_observer_list, get_observers_data, extract_observer_json, clean_observer])

inat_data_module = Module('inat_data',[inat_observers], always_run= True )
