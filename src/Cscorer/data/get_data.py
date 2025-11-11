# Main file to get data 
from ..core import PipelineData, read_config, StepStatus
from ..utils.duckdb import import_csv_to_db
from .factory import create_query
from pathlib import Path
import asyncio
import time

async def get_all_data(data:PipelineData):
    
    
    #Prep citizen specific predicates
    cs_predicates = {'BASIS_OF_RECORD': 'HUMAN_OBSERVATION'}
    
    #Get expert datasets keys from config
    datasets = data.config['datasets']
    dataset_keys = []
    for dataset in datasets.values():
        dataset_keys.append(dataset['key'])

    expert_predicates = {"DATASET_KEY" : dataset_keys }

    cs_query = await create_gbif_query(data, name= 'citizen', predicates= cs_predicates)
    expert_query = await create_gbif_query(data, name= 'expert', predicates= expert_predicates)
    
    async with asyncio.TaskGroup() as tg:
        if not data.step_status[f"{cs_query.name}"] == StepStatus.local:
            cs_data_task = tg.create_task(cs_query.run(data))

        if not data.step_status[f"{expert_query.name}"] == StepStatus.local:
            expert_data_task = tg.create_task(expert_query.run(data))

            
    if cs_data_task:
        cs_data = cs_data_task.result()
    else: 
        cs_data = data.storage[f"{cs_query.name}"]["raw_data"]
        
    if expert_data_task:
        expert_data = cs_data_task.result()
    else: 
        expert_data = data.storage[f"{expert_query.name}"]["raw_data"]
        

    #expert_data = tg.create_task(expert_query.run(data, "get_expert_data"))
    print(cs_data)
    print(expert_data)
    quit()
    
    async with asyncio.TaskGroup() as tg:
        citizen_table = tg.create_task(import_csv_to_db(data.con, cs_data.result(), schema= 'gbif_raw', table= 'citizen' ))
        #expert_table = tg.create_task(import_csv_to_db(data.con, expert_data.result(), schema= 'gbif_raw', table= 'expert' ))

    print(citizen_table.result())
    quit()
        

async def create_gbif_query(data:PipelineData, name:str, predicates:dict = None):
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
    

async def get_citizen_data(data:PipelineData):
    step_name = 'get_citizen_data'
    # GBIF QUERY
    if step_name not in data.step_status.keys() or data.step_status[f'{step_name}'] != StepStatus.completed:
        # Init step status
        data.update_step_status(step_name, StepStatus.init)
        #Retrieve base configs 
        #Create query
        cs_query = create_query('gbif', config=  gbif_config)
        #print(cs_query.predicate)
        return cs_query
        #Run the request to gbif
        cs_data = asyncio.create_task(gbif_query.run(data, step_name))
        #Commit received data to db
        cs_table = import_csv_to_db(data.con, cs_data, schema= 'gbif_raw', table= 'citizen' )
        #Set step completed
        if cs_table:
             data.step_status[f'{step_name}'] != StepStatus.completed
        
async def get_expert_data(data:PipelineData):
    step_name = 'get_expert_data'
    
    if step_name not in data.step_status.keys() or data.step_status[f'{step_name}'] != StepStatus.completed:
        # Init step status
        data.update_step_status(step_name, StepStatus.init)
        #Retrieve base configs 
        gbif_config = data.config['gbif']
        
        #Get expert datasets keys from config
        datasets = data.config['datasets']
        dataset_keys = []
        for dataset in datasets.values():
            dataset_keys.append(dataset['key'])
        #Create query
        expert_query = create_query('gbif', gbif_config)
        
        #Add specific expert data configs
        expert_query.predicate.add_field(key ='DATASET_KEY', value = dataset_keys)
        
        
        return expert_query
        #Run the request to gbif
        expert_data = asyncio.create_task(gbif_query.run(data, step_name))

    

async def get_inaturalist_metadata(data:PipelineData):
    step_name = 'get_inaturalist_metadata'

    pass

async def get_environmental_data(data:PipelineData):
    step_name = 'get_environmental_data'

    pass



