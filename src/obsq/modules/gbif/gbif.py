from . import GbifLoader, gbif_clean_submodule
from ...pipeline import *
from ...steps import *
from ...utils import import_csv_to_db

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
collect_citizen_data = GbifLoader('collect_citizen_data')
collect_expert_data = GbifLoader('collect_expert_data')
sm_collect_all_gbif = SubModule("collect_all_gbif",[collect_citizen_data,
                                                    collect_expert_data])

# STORE
sm_store_all_gbif = SubModule("store_all_gbif",[store_gbif_citizen_csv,
                                                store_gbif_expert_csv])


gbif_ingest = Module("ingest_gbif",[create_custom_predicates,
                                           sm_collect_all_gbif,
                                           sm_store_all_gbif])

# Create observers table 
extract_all_observers = DataBaseQuery("extract_all_observers", query_name= "gbif_extract_all_observers")
#Get citizen experts 
get_citizen_expert = DataBaseQuery("get_citizen_expert", query_name= "gbif_get_citizen_expert")

get_citizen_observers = DataBaseQuery("get_citizen_observers", query_name= "gbif_get_citizen_observers")

# Send observations from citizen expert to expert table
citizen_occ_to_expert = DataBaseQuery("citizen_occ_to_expert", query_name= "gbif_citizen_occ_to_expert" )

filter_observers_sm = SubModule("filter_observers",[extract_all_observers,
                                                get_citizen_expert,
                                                get_citizen_observers,
                                                citizen_occ_to_expert])


# FULL MODULE 
gbif_preprocess = Module("preprocess_gbif", [
                                           gbif_clean_submodule,
                                            filter_observers_sm

                                           ])

    