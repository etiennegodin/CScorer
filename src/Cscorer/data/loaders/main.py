# Main file to get data 
from ...pipeline import Pipeline, PipelineModule, PipelineSubmodule,PipelineStep, StepStatus
from ...utils.duckdb import import_csv_to_db, create_schema
from .factory import create_query
from pathlib import Path
import asyncio
from .gee import upload_points
from pprint import pprint

### Create instances for each class of data and run their queries

async def data_loaders(pipe:Pipeline, submodule:PipelineSubmodule):
    
    submodule.add_step(PipelineStep( "data_load_gbif_citizen", func = data_load_gbif_main))
    submodule.add_step(PipelineStep("data_load_gbif_expert", func = data_load_gbif_main))
    #pipe.add_step(submodule, "data_load_inat_occurence", func = data_load_inat_occurence)
    submodule.add_step(PipelineStep("data_load_inat_observer", func = data_load_inat_observer))
    submodule.add_step(PipelineStep("data_load_points", func = data_load_points))

    
    async with asyncio.TaskGroup() as tg:
        tg.create_task(submodule.steps["data_load_gbif_citizen"].run(pipe))
        tg.create_task(submodule.steps["data_load_gbif_expert"].run(pipe))
        
    async with asyncio.TaskGroup() as tg:
        tg.create_task(submodule.steps["data_load_inat_observer"].run(pipe))
        gee_upload_task = tg.create_task(submodule.steps["data_load_points"].run(pipe))


    points_dict = submodule.steps["data_load_points"].storage['points']

    for name, points in points_dict.items():
        submodule.add_step(PipelineStep(f"data_load_sample_gee_{name}", func = data_load_sample_gee))
        await submodule.steps[f"data_load_sample_gee_{name}"].run(pipe, points = points)
    
async def data_load_gbif_main(pipe:Pipeline, step:PipelineStep):
    subcategory = step.name.split(sep="_")[-1]
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
    table = await import_csv_to_db(pipe.con, result, schema= 'gbif_raw', table= subcategory, geo = True)

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
        
async def data_load_inat_occurence(pipe:Pipeline, step:PipelineStep):
    con = pipe.con
    # Create query 
    inatOcc_query = create_query('inatOcc', name = step.name)
        
    #Return url for 
    occurence = await inatOcc_query.run(pipe,step)    

async def data_load_inat_observer(pipe:Pipeline, step:PipelineStep):
    con = pipe.con
    # Create query 
    inatObs_query = create_query('inatObs', name = step.name)
    #Init step
    #Return url for 
    oberver_table = await inatObs_query.run(pipe,step)    

async def data_load_points(pipe:Pipeline, step:PipelineStep):
    #Create table schema on db 
    create_schema(pipe.con, schema = 'gee')
        # Get point to gee
    points_list = await upload_points(pipe,step)
    return points_list
    
async def data_load_sample_gee(pipe:Pipeline, step:PipelineStep, points:str):
    gee_query = create_query('gee', pipe, name = step.name, points=points)
    #Create list of queries (one for each set of occurences)
    await gee_query.run(pipe, step)