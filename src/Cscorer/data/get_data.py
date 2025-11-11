# Main file to get data 
from ..core import PipelineData, read_config, StepStatus
from ..utils.duckdb import import_csv_to_db
from .factory import create_query
from pathlib import Path
import asyncio



async def get_all_data(data:PipelineData):
    cs_query = await get_citizen_data(data)
    expert_query = await get_expert_data(data)
    
    print(cs_query)
    print(expert_query)
    
    
    print(cs_query.predicate)
    print("\n"*10)
    print(expert_query.predicate)

    async with asyncio.TaskGroup() as tg:
        cs_data = tg.create_task(cs_query.run(data, "get_citizen_data"))
        #expert_data = tg.create_task(expert_query.run(data, "get_expert_data"))
    print("@"*100, "RESULTS")
    print(cs_data.result())
    quit()
    
    async with asyncio.TaskGroup() as tg:
        expert_table = tg.create_task(import_csv_to_db(data.con, cs_data.result(), schema= 'gbif_raw', table= 'citizen' ))
        expert_table = tg.create_task(import_csv_to_db(data.con, expert_data.result(), schema= 'gbif_raw', table= 'expert' ))

        quit()
        

    

async def get_citizen_data(data:PipelineData):
    step_name = 'get_citizen_data'
    # GBIF QUERY
    if step_name not in data.step_status.keys() or data.step_status[f'{step_name}'] != StepStatus.completed:
        # Init step status
        data.update_step_status(step_name, StepStatus.init)
        #Retrieve base configs 
        gbif_config = data.config['gbif']
        #Create query
        cs_query = create_query('gbif', gbif_config)
        cs_query.predicate.add_field(key = 'BASIS_OF_RECORD', value = 'HUMAN_OBSERVATION')
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



