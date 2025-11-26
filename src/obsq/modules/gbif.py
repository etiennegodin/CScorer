from obsq.pipeline import PipelineContext, Module, SubModule, step
from ..steps import GbifLoader, sm_gbif_clean
from ..utils.duckdb import import_csv_to_db
from ..utils.sql import read_sql_template
from pathlib import Path



@step 
def create_custom_predicates(context:PipelineContext):
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
async def store_gbif_citizen_csv(context:PipelineContext):
    step_output = context.get_step_output("collect_citizen_data")
    if step_output is not None:
        file = step_output['output']
        table =  import_csv_to_db(context.con, file, "raw", "gbif_citizen", geo = True)
        return table
    
@step
async def store_gbif_expert_csv(context:PipelineContext):
    step_output = context.get_step_output("collect_expert_data")
    if step_output is not None:
        file = step_output['output']
        table = import_csv_to_db(context.con, file, "raw", "gbif_expert", geo = True)
        return table


# COLLECT
collect_citizen_data = GbifLoader('collect_citizen_data')
collect_expert_data = GbifLoader('collect_expert_data')
sm_collect_all_gbif = SubModule("collect_all_gbif",[collect_citizen_data, collect_expert_data])

# STORE
sm_store_all_gbif = SubModule("store_all_gbif",[store_gbif_citizen_csv, store_gbif_expert_csv])






m_collect_gbif = Module("ingest_gbif",[create_custom_predicates, sm_collect_all_gbif, sm_store_all_gbif, sm_gbif_clean])

    