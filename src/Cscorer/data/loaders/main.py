# Main file to get data 
from ...core import Pipeline, PipelineModule,PipelineStep, StepStatus
from ...utils.duckdb import import_csv_to_db, create_schema
from .factory import create_query
from pathlib import Path
import asyncio
import time
import aiohttp
from .gee import upload_points

### Create instances for each class of data and run their queries

async def set_loaders(pipe:Pipeline, module:PipelineModule):
    submodule = pipe.add_submodule(module,'loaders')

    # maybe like the gbif async orchestrator with task group 
    
    # still need to run in order for occurence (gbif -> inat_observers) but env data can run concurrently 
    
    # gbif
    #   inat observer
    # inat occurences ( requires download file )
    # env 
    pipe.add_step(submodule, "data_load_gbif_main", func = data_load_gbif_main)
    
    
    print(pipe.__dict__)
    #loaders.run_submodule(data = )

    #main orchestrator
    #asyncio.run(data_load_gbif_main(data)) 
    #asyncio.run(data_load_inat_occurence(data))
    #asyncio.run(data_load_inat_observer(data))
    #asyncio.run(data_load_gee(data)) 
    pass

async def data_load_gbif_main(pipe:Pipeline):
    
    step_name = "data_load_gbif_main"
    #Temp assignement for async tasks 
    cs_data_task = None
    expert_data_task = None
    
    #Prep citizen specific predicates
    cs_predicates = {'BASIS_OF_RECORD': 'HUMAN_OBSERVATION'}
    
    #Prep expert specific predicates
    datasets = pipe.config['gbif_datasets']
    dataset_keys = []
    for dataset in datasets.values():
        dataset_keys.append(dataset['key'])
    expert_predicates = {"DATASET_KEY" : dataset_keys }

    # Create queries 
    cs_query = await _create_gbif_loader(pipe, name= 'citizen', predicates= cs_predicates)
    expert_query = await _create_gbif_loader(pipe, name= 'expert', predicates= expert_predicates)
    
    #Lauch async concurrent queries 
    async with asyncio.TaskGroup() as tg:
        if not data.step_status[f"{cs_query.name}"] == StepStatus.local:
            cs_data_task = tg.create_task(cs_query.run(data))

        if not data.step_status[f"{expert_query.name}"] == StepStatus.local:
            expert_data_task = tg.create_task(expert_query.run(data))

    #Read data from async tasks or from storage 
    if cs_data_task is not None:cs_data = cs_data_task.result()
    else: cs_data = data.storage[f"{cs_query.name}"]["raw_data"]
    if expert_data_task is not None:expert_data = expert_data_task.result()
    else: expert_data = data.storage[f"{expert_query.name}"]["raw_data"]
        
    # Commit local .csv to db 
    cs_table = await import_csv_to_db(data.con, cs_data, schema= 'gbif_raw', table= 'citizen', geo = True)
    expert_table = await import_csv_to_db(data.con, expert_data, schema= 'gbif_raw', table= 'expert', geo = True )

    # Flag as completed
    if cs_table:
        data.storage[f"{cs_query.name}"]["db"] = cs_table
        data.step_status[f'{cs_query.name}'] = StepStatus.completed   
    if expert_table:
        data.storage[f"{expert_query.name}"]["db"] = expert_table
        data.step_status[f'{expert_query.name}'] = StepStatus.completed    
        
async def _create_gbif_loader(pipe:Pipeline, name:str, predicates:dict = None):
    step_name = f"data_load_gbif_{name}"
    gbif_config = data.config['gbif']
    
    if not isinstance(predicates, (dict, None)):
        raise ValueError("Pedicates must be dict")
    
    data.logger.info(f"Creating new instance of {step_name}")

    # Create query
    query = create_query('gbif', name = step_name, config = gbif_config)
    
    #Add additonnal predicates
    for key, value in predicates.items():
        query.predicate.add_field(key = key, value = value)
    
    #Init step
    data.init_new_step(step_name)
        
    return query   
        
async def data_load_inat_occurence(pipe:Pipeline):
    step_name = "data_load_inat_occurence"
    con = data.con
    # Create query 
    inatOcc_query = create_query('inatOcc', name = step_name)
    
    #Init step
    data.init_new_step(step_name)
    
    #Return url for 
    occurence = await inatOcc_query.run(data)    

async def data_load_inat_observer(pipe:Pipeline):
    step_name = "data_load_inat_observer"
    con = data.con
    # Create query 
    inatObs_query = create_query('inatObs', name = step_name)
    #Init step
    data.init_new_step(step_name)
    #Return url for 
    oberver_table = await inatObs_query.run(data, limit = data.config['inat_api']['limit'], overwrite = data.config['inat_api']['overwrite'])    

async def data_load_gee(pipe:Pipeline):
    # Get point to gee
    points_list = await upload_points(data)
    #Create table schema on db 
    create_schema(data.con, schema = 'gee')
    
    #Create list of queries (one for each set of occurences)
    gee_queries = [create_query('gee', data, points=points) for points in points_list]
    for query in gee_queries:
        await query.run(data)