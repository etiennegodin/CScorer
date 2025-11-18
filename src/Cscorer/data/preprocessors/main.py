from ...pipeline import Pipeline, PipelineModule, PipelineSubmodule,PipelineStep, StepStatus
from ...utils.duckdb import create_schema
from ...utils.sql import read_sql_file
import duckdb
import asyncio

async def data_preprocessors(pipe:Pipeline, submodule:PipelineSubmodule):
    schema = "preprocessed"
    create_schema(pipe.con, schema)
    
    submodule.reset_steps()
    
    template = PipelineStep( "template", func = data_prepro_template)

    clean_gbif_citizen = PipelineStep( "clean_gbif_citizen", func = execute_sql_query)
    clean_gbif_expert = PipelineStep( "clean_gbif_expert", func = execute_sql_query)

    submodule.add_step(template)
    submodule.add_step(clean_gbif_citizen)
    submodule.add_step(clean_gbif_expert)

    tasks = [asyncio.create_task(step.run(pipe)) for step in submodule.steps.values()]
    await asyncio.gather(*tasks)

async def data_prepro_template(pipe:Pipeline, step:PipelineStep):
    pass

async def execute_sql_query(pipe:Pipeline, step:PipelineStep):
    con = pipe.con
    query = read_sql_file(f'{step.name}', local= True)
    try:
        con.execute(query)
    except Exception as e:
        pipe.logger.error(f"Failed to run query : {e}")
        step.status = StepStatus.failed
        
    step.status = StepStatus.completed
    
async def data_prepro_missing_values(pipe:Pipeline, step:Pipeline):
    pass


async def data_prepro_temporal_filter(pipe:Pipeline, step:Pipeline):
    pass





