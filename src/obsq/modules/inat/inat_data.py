from .inat_api import inatApiClient
from ...steps import DataBaseLoader
from ...pipeline import PipelineContext,Module, step

get_observer_list_query = """SELECT * FROM """

get_observer_list = DataBaseLoader('get_observer_list', columns='recordedBy',from_table='raw.citizen_observers', limit = 10, return_type= 'list')

get_observers_data = inatApiClient('inat_observers',
                                   endpoint= 'users/autocomplete/q?=',
                                   params_key= None,
                                   items_from = 'get_observer_list',
                                   per_page=1,
                                   chunk_size=1,
                                   overwrite_table= True)

inat_data_module = Module('inat_data',[get_observer_list, get_observers_data] )
