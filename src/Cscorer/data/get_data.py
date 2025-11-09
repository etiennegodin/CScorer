# Main file to get data 
from ..core import PipelineData, read_config, StepStatus
from ..utils.duckdb import import_csv
from .factory import create_query
from pathlib import Path
import asyncio

async def get_gbif(data:PipelineData):
    
    # GBIF QUERY
    if "gbif_query" not in data.step_status.keys() or data.step_status['gbif_query'] != StepStatus.completed:
        
        gbif_config = data.config['gbif']
        config = data.get('gbif_query_config')
        gbif_query = create_query('gbif', gbif_config)
        gbif_data = await gbif_query.run(data)
        gbif_table = import_csv(data.con, gbif_data, schema= 'gbif', table= 'raw' )
        if gbif_table:
             data.step_status['gbif_query'] != StepStatus.completed
             
        
        