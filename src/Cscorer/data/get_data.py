# Main file to get data 
from ..core import PipelineData, read_config, StepStatus
from ..utils.duckdb import import_csv_to_db
from .factory import create_query
from pathlib import Path
import asyncio

async def get_citizen_data(data:PipelineData):
    step_name = 'get_citizen_data'
    # GBIF QUERY
    if step_name not in data.step_status.keys() or data.step_status[f'{step_name}'] != StepStatus.completed:
        # Init step status
        data.update_step_status(step_name, StepStatus.init)
        #Retrieve base configs 
        gbif_config = data.config['gbif']
        #Create query
        gbif_query = create_query('gbif', gbif_config)
        gbif_query.predicate.add_field('BASIS_OF_RECORD', 'HUMAN_OBSERVATION')

        #gbif_query.predicate['predicates'].append({'test'})
        print(gbif_query.predicate)

        return
        #Run the request to gbif
        gbif_data = await gbif_query.run(data, step_name)
        #Commit received data to db
        gbif_table = import_csv_to_db(data.con, gbif_data, schema= 'gbif_raw', table= 'citizen' )
        #Set step completed
        if gbif_table:
             data.step_status['gbif_query'] != StepStatus.completed
        
async def get_expert_data(data:PipelineData):
    step_name = 'get_expert_data'
    
    if step_name not in data.step_status.keys() or data.step_status[f'{step_name}'] != StepStatus.completed:
        # Init step status
        data.update_step_status(step_name, StepStatus.init)
        #Retrieve base configs 
        gbif_config = data.config['gbif']
        #Add specific expert data configs
        
        #Create query
        gbif_query = create_query('gbif', gbif_config)
        #Run the request to gbif
        gbif_data = await gbif_query.run(data, step_name)
        #Commit received data to db
        gbif_table = import_csv_to_db(data.con, gbif_data, schema= 'gbif_raw', table= 'expert' )
        #Set step completed
        if gbif_table:
             data.step_status['gbif_query'] != StepStatus.completed
    

async def get_inaturalist_metadata(data:PipelineData):
    step_name = 'get_inaturalist_metadata'

    pass

async def get_environmental_data(data:PipelineData):
    step_name = 'get_environmental_data'

    pass