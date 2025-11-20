from ...pipeline import Pipeline, PipelineModule, PipelineSubmodule,PipelineStep, StepStatus
from ...utils.duckdb import create_schema
from ...utils.sql import read_sql_template, read_sql_file, simple_sql_query
import duckdb
import asyncio
from pathlib import Path
async def data_preprocessors(pipe:Pipeline, submodule:PipelineSubmodule):
    schema = "preprocessed"
    create_schema(pipe.con, schema)
    sql_folder = Path(__file__).parent
    clean_gbif_citizen = PipelineStep( "clean_gbif_citizen", func = clean_gbif_occurences)
    clean_gbif_expert = PipelineStep( "clean_gbif_expert", func = clean_gbif_occurences)
    merge_inatOccurences = PipelineStep( "merge_inatOccurences", func = simple_sql_query)
    matchGbifDatasets = PipelineStep( "matchGbifDatasets", func = simple_sql_query)

    submodule.add_step(clean_gbif_citizen)
    submodule.add_step(clean_gbif_expert)
    
    async with asyncio.TaskGroup() as tg:
        tg.create_task(submodule.steps['clean_gbif_citizen'].run(pipe, sql_folder = sql_folder))
        tg.create_task(submodule.steps['clean_gbif_expert'].run(pipe, sql_folder = sql_folder))
    
    submodule.add_step(merge_inatOccurences)
    submodule.add_step(matchGbifDatasets)
    
    await submodule.steps['merge_inatOccurences'].run(pipe, sql_folder = sql_folder)
    await submodule.steps['matchGbifDatasets'].run(pipe, sql_folder = sql_folder)

async def data_prepro_template(pipe:Pipeline, step:PipelineStep):
    pass

    
async def clean_gbif_occurences(pipe:Pipeline, step:PipelineStep, sql_folder:Path):
    con = pipe.con
    target_table_name = step.name.split(sep='_', maxsplit=1)[-1]
    source_table_name =  f"gbif_raw.{step.name.split(sep='_')[-1]}"
    file_name = 'clean_gbif'
    file_path = sql_folder / f"{file_name}.sql"
    
    template = read_sql_template(file_path)
    query = template.render(target_table_name = target_table_name, source_table_name = source_table_name)
    
    try:
        con.execute(query)
    except Exception as e:
        pipe.logger.error(f"Failed to run query : {e}")
        step.status = StepStatus.failed
    step.status = StepStatus.completed





