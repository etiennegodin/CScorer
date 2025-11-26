from obsq.pipeline import PipelineContext, Module, step
from ..steps import GbifLoader
from ..utils.duckdb import import_csv_to_db

#Citizen data loader
collect_citizen_data = GbifLoader('collect_citizen_data', predicates = {'BASIS_OF_RECORD': 'HUMAN_OBSERVATION'})

collect_expert_data = GbifLoader('collect_expert_data', predicates={})


@step 
def create_expert_custom_predicates(context:PipelineContext):
    #Prep expert specific predicates
    datasets = context.config['gbif_datasets']
    dataset_keys = []
    for dataset in datasets.values():
        dataset_keys.append(dataset['key'])
    predicates = {"DATASET_KEY" : dataset_keys }
    return predicates

@step
def store_gbif_citizen_csv(context:PipelineContext):
    step_output = context.get_step_output("collect_citizen_data")
    if step_output is not None:
        file = step_output['output']
        table = import_csv_to_db(context.con, file, "raw", "gbif_citizen", geo = True)
        return table
@step
def store_gbif_expert_csv(context:PipelineContext):
    step_output = context.get_step_output("collect_expert_data")
    if step_output is not None:
        file = step_output['output']
        table = import_csv_to_db(context.con, file, "raw", "gbif_expert", geo = True)
        return table



m_collect_gbif = Module("collect_gbif",[collect_citizen_data,store_gbif_citizen_csv,create_expert_custom_predicates])

    