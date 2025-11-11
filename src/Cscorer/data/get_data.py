# Main file to get data 
from ..core import PipelineData, read_config, StepStatus
from ..utils.duckdb import import_csv_to_db
from .factory import create_query
from pathlib import Path
import asyncio
import time
import aiohttp


async def get_gbif_data(data:PipelineData):

    #Temp assignement for async tasks 
    cs_data_task = None
    expert_data_task = None
    
    #Prep citizen specific predicates
    cs_predicates = {'BASIS_OF_RECORD': 'HUMAN_OBSERVATION'}
    
    #Prep expert specific predicates
    datasets = data.config['datasets']
    dataset_keys = []
    for dataset in datasets.values():
        dataset_keys.append(dataset['key'])
    expert_predicates = {"DATASET_KEY" : dataset_keys }

    # Create queries 
    cs_query = await _create_gbif_query(data, name= 'citizen', predicates= cs_predicates)
    expert_query = await _create_gbif_query(data, name= 'expert', predicates= expert_predicates)
    
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
    cs_table = await import_csv_to_db(data.con, cs_data, schema= 'gbif_raw', table= 'citizen' )
    expert_table = await import_csv_to_db(data.con, expert_data, schema= 'gbif_raw', table= 'expert' )

    # Flag as completed
    if cs_table:
        data.step_status[f'{cs_query.name}'] = StepStatus.completed    

    if expert_table:
        data.step_status[f'{expert_query.name}'] = StepStatus.completed    
        
        
async def get_inaturalist_data(data:PipelineData):
    con = data.con
    observer_list = con.execute()
    

    pass

async def get_environmental_data(data:PipelineData):
    step_name = 'get_environmental_data'

    pass

async def _create_gbif_query(data:PipelineData, name:str, predicates:dict = None):
    step_name = f"gbif_query_{name}"
    gbif_config = data.config['gbif']
    
    if not isinstance(predicates, (dict, None)):
        raise ValueError("Pedicates must be dict")
    
    data.logger.info(f"Creating new instance of {step_name}")

    # Create query
    query = create_query('gbif', name = step_name, config = gbif_config)
    
    #Add additonnal predicates
    for key, value in predicates.items():
        query.predicate.add_field(key = key, value = value)
    
    #Set StepStatus        
    
    if not step_name in data.storage.keys():
        data.logger.info(f"First time running {step_name}, creating storage and step status")
        data.set(step_name,  {'init' : time.time()})
        data.update_step_status(step_name, StepStatus.init)
        
    return query



