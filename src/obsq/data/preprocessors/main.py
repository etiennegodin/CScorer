from ...pipeline import Pipeline, PipelineModule, PipelineSubmodule,PipelineStep, StepStatus
from ...utils.duckdb import create_schema
from ...utils.sql import read_sql_template, read_sql_file, simple_sql_query
import duckdb
import asyncio
from pathlib import Path
from ..loaders.factory import create_query
from ..loaders.inat import fields_to_string

async def data_preprocessors(pipe:Pipeline, submodule:PipelineSubmodule):
    create_schema(pipe.con, 'clean')
    create_schema(pipe.con, 'preprocessed')

    sql_folder = Path(__file__).parent
    #Clean gbif occurrences
    clean_gbif_citizen = PipelineStep( "clean_gbif_citizen", func = clean_gbif_occurences)
    clean_gbif_expert = PipelineStep( "clean_gbif_expert", func = clean_gbif_occurences)
    
    clean_gbif_expert = PipelineStep( "clean_gbif_expert", func = clean_gbif_occurences)

        
    # Get additionnal expert users from inat
    extract_inat_expert = PipelineStep( "extract_inat_expert", func = simple_sql_query)
    
    #Remove expert occ from citizen, populate in expert_occ
    shuffle_inat_expert_obs = PipelineStep( "shuffle_inat_expert_obs", func = simple_sql_query)
    
    #Extract json response for users fields
    extract_observer_json = PipelineStep( "extract_observer_json", func = simple_sql_query)

    submodule.add_step(clean_gbif_citizen)
    submodule.add_step(clean_gbif_expert)
    submodule.add_step(extract_inat_expert)
    submodule.add_step(shuffle_inat_expert_obs)
    submodule.add_step(extract_observer_json)
    
    async with asyncio.TaskGroup() as tg:
        tg.create_task(submodule.steps['clean_gbif_citizen'].run(pipe, sql_folder = sql_folder))
        tg.create_task(submodule.steps['clean_gbif_expert'].run(pipe, sql_folder = sql_folder))
        
    async with asyncio.TaskGroup() as tg:
        tg.create_task(extract_inat_expert.run(pipe, sql_folder = sql_folder))
        tg.create_task(extract_observer_json.run(pipe, sql_folder = sql_folder))

    await shuffle_inat_expert_obs.run(pipe, sql_folder = sql_folder)
    
 
async def clean_gbif_occurences(pipe:Pipeline, step:PipelineStep, sql_folder:Path):
    con = pipe.con
    target_table_name = step.name.split(sep='_', maxsplit=1)[-1]
    source_table_name =  f"raw.{step.name.split(sep='_',maxsplit=1)[-1]}"
    file_name = 'clean_gbif'
    file_path = sql_folder / f"{file_name}.sql"
    
    template = read_sql_template(file_path)
    query = template.render(target_table_name = target_table_name, source_table_name = source_table_name)
    
    try:
        con.execute(query)
    except Exception as e:
        pipe.logger.error(f"FAILED to run query : {e}")
        step.status = StepStatus.FAILED
    step.status = StepStatus.COMPLETED
    