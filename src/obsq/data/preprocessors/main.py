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
        
    # Get additionnal expert users from inat
    extract_inat_expert = PipelineStep( "extract_inat_expert", func = simple_sql_query)
    
    #Remove expert occ from citizen, populate in expert_occ
    shuffle_inat_expert_obs = PipelineStep( "shuffle_inat_expert_obs", func = simple_sql_query)
    
    #Extract json response for users fields
    extract_observer_json = PipelineStep( "extract_observer_json", func = simple_sql_query)
    
    #Query iNat API with reduced gbif citizen occurences 
    inatOccMetadata = PipelineStep("get_inatOccMetadata", func = get_inatOccMetadata)
    
    # Query species Info with reducded gbif citizen occurences 
    inatSpeciesData = PipelineStep("get_inatOccMetadata", func = get_inatSpeciesData)


    # Label citizen observations matching expert occurrences
    matchGbifDatasets = PipelineStep( "matchGbifDatasets", func = simple_sql_query)

    #merge_inatOccurences = PipelineStep( "merge_inatOccurences", func = simple_sql_query)
    
    submodule.add_step(clean_gbif_citizen)
    submodule.add_step(clean_gbif_expert)
    submodule.add_step(extract_inat_expert)
    submodule.add_step(shuffle_inat_expert_obs)
    submodule.add_step(extract_observer_json)
    
    submodule.add_step(inatOccMetadata)
    submodule.add_step(inatSpeciesData)

    #submodule.add_step(merge_inatOccurences)
    submodule.add_step(matchGbifDatasets)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(submodule.steps['clean_gbif_citizen'].run(pipe, sql_folder = sql_folder))
        tg.create_task(submodule.steps['clean_gbif_expert'].run(pipe, sql_folder = sql_folder))
        
    async with asyncio.TaskGroup() as tg:
        tg.create_task(extract_inat_expert.run(pipe, sql_folder = sql_folder))
        tg.create_task(extract_observer_json.run(pipe, sql_folder = sql_folder))

    await shuffle_inat_expert_obs.run(pipe, sql_folder = sql_folder)
    
    async with asyncio.TaskGroup() as tg:
        tg.create_task(inatSpeciesData.run(pipe))
        tg.create_task(inatOccMetadata.run(pipe))
    
    #await matchGbifDatasets.run(pipe, sql_folder = sql_folder)


async def get_inatOccMetadata(pipe:Pipeline, step:PipelineStep):
    fields = {
        "id":True,
          "annotations" : {
              "controlled_attribute_id" : True,
              "controlled_attribute" : { "label" : True},
              "user_id" : True,
              "name": True},
          "body":True,
          "description": True,
          'reviewed_by' : True,
          "comments_count" : True,
          "comments" : {"id" : True,
                        "body" : True,
                        "user" : {"id": True, "name":True}},
          "num_identification_agreements" :True,
          "num_identification_disagreements" : True,
          "owners_identification_from_vision" : True,
          "identifications_count":True,
          "identifications" : { 
              "id": True,
              "created_at" : True,
              "user" : {"id":True, "name":True},
              "category" : True,
              "current" : True,
              "own_observation" : True,
              "taxon_change": True,
              "vision" : True,
              "disagreement" : True,
              },
          "application" : {"name" : True}       
    }
    

    query = create_query('inat', pipe, name = 'inat_observations' )
    key = "occurrenceID"
    get_occurencesIDs = f"""
            SELECT {key},
            FROM preprocessed.gbif_citizen_filtered
    """
    occurenceURLs = pipe.con.execute(get_occurencesIDs).df()[key]
    occurenceIDs = occurenceURLs.apply(lambda x: x.split(sep='/')[-1]).to_list()
    occurenceIDs = occurenceIDs[:10]
    
    fields_string = f"({fields_to_string(fields)})"

    await query.run(pipe,step, occurenceIDs,'observations', fields_string )    

    
async def get_inatSpeciesData(pipe, step):
    
    #query = create_query('inat', name = 'inat_species' )
    
    pass

 
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
        pipe.logger.error(f"Failed to run query : {e}")
        step.status = StepStatus.failed
    step.status = StepStatus.completed
    





