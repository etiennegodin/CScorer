from . import GbifApi
from ....pipeline import *
from ....utils import import_csv_to_db

@step 
def create_custom_predicates(context:PipelineContext):
    """
    Docstring for create_custom_predicates
    
    :param context: Description
    :type context: PipelineContext
    """
    predicates = {}
    #Prep citizen predicates
    predicates['collect_citizen_data'] = {'BASIS_OF_RECORD': 'HUMAN_OBSERVATION'}
    
    #Prep expert specific predicates
    datasets = context.config['gbif_datasets']
    dataset_keys = []
    for dataset in datasets.values():
        dataset_keys.append(dataset['key'])
    predicates['collect_expert_data'] = {"DATASET_KEY" : dataset_keys }
    
    return predicates

@step
async def store_gbif_citizen_csv(context:PipelineContext)->str:
    step_output = context.get_step_output("collect_citizen_data")
    if step_output is not None:
        file = step_output['output']
        table =  import_csv_to_db(context.con, file, "raw", "gbif_citizen", geo = True)
        return table
    
@step
async def store_gbif_expert_csv(context:PipelineContext)->str:
    step_output = context.get_step_output("collect_expert_data")
    if step_output is not None:
        file = step_output['output']
        table = import_csv_to_db(context.con, file, "raw", "gbif_expert", geo = True)
        return table

# COLLECT
collect_citizen_data = GbifApi('collect_citizen_data')
collect_expert_data = GbifApi('collect_expert_data')


ingest_gbif_module = Module("ingest_gbif",[create_custom_predicates,
                                           collect_citizen_data,
                                           collect_expert_data,
                                           store_gbif_citizen_csv,
                                           store_gbif_expert_csv
                                           ])
