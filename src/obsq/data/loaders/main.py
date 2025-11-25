# Main file to get data 
from ...pipeline import Pipeline, PipelineModule, PipelineSubmodule,PipelineStep, StepStatus
from ...utils.duckdb import import_csv_to_db, create_schema
from .factory import create_query
import asyncio

### Create instances for each class of data and run their queries

async def data_loaders(pipe:Pipeline, submodule:PipelineSubmodule):
    create_schema(pipe.con, "raw")
    submodule.add_step(PipelineStep( "data_load_gbif_citizen", func = data_load_gbif_main))
    submodule.add_step(PipelineStep("data_load_gbif_expert", func = data_load_gbif_main))

    async with asyncio.TaskGroup() as tg:
        tg.create_task(submodule.steps["data_load_gbif_citizen"].run(pipe))
        tg.create_task(submodule.steps["data_load_gbif_expert"].run(pipe))
        

async def data_load_gbif_main(pipe:Pipeline, step:PipelineStep):
    table_name = step.name.split(sep="_", maxsplit=2)[-1]
    subcategory = table_name.split(sep="_")[-1]
    
    #Temp assignement for async tasks 
    task = None
    if subcategory == "citizen":
    #Prep citizen specific predicates
        predicates = {'BASIS_OF_RECORD': 'HUMAN_OBSERVATION'}
    elif subcategory == "expert":
        #Prep expert specific predicates
        datasets = pipe.config['gbif_datasets']
        dataset_keys = []
        for dataset in datasets.values():
            dataset_keys.append(dataset['key'])
        predicates = {"DATASET_KEY" : dataset_keys }
    else:
        raise NotImplementedError
    # Create queries 
    query = await _create_gbif_loader(pipe, step, name= subcategory, predicates= predicates)
    
    #Lauch async concurrent queries 
    if not step.status == StepStatus.local:
        result = await query.run(pipe,step)
        
    # Commit local .csv to db 
    table = await import_csv_to_db(pipe.con, result, schema= 'raw', table= table_name, geo = True)

    # Flag as completed
    if table:
        step.storage["db"] = table
        step.status = StepStatus.completed   

        
async def _create_gbif_loader(pipe:Pipeline, step:PipelineStep, name:str, predicates:dict = None):
    gbif_config = pipe.config['gbif']
    
    if not isinstance(predicates, (dict, None)):
        raise ValueError("Pedicates must be dict")
    
    pipe.logger.info(f"Creating new instance of {step.name}")

    # Create query
    query = create_query('gbif', name = step.name, config = gbif_config)
    
    #Add additonnal predicates
    for key, value in predicates.items():
        query.predicate.add_field(key = key, value = value)
            
    return query   
        
