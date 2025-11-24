from ...pipeline import Pipeline, PipelineModule, PipelineSubmodule,PipelineStep, StepStatus
from ...utils.duckdb import create_schema
from ...utils.sql import read_sql_template, read_sql_file, simple_sql_query
import duckdb
import asyncio
from pathlib import Path
from ..loaders.factory import create_query
from ..loaders.inat import fields_to_string

async def data_post_preprocess(pipe:Pipeline, submodule:PipelineSubmodule):

    sql_folder = Path(__file__).parent
    submodule.reset_steps()

    #Query iNat API with reduced gbif citizen occurences 
    inatOccMetadata = PipelineStep("get_inatOccMetadata", func = get_inatOccMetadata)
    
    # Query species Info with reducded gbif citizen occurences 
    inatSpeciesData = PipelineStep("get_inatOccMetadata", func = get_inatSpeciesData)

    # Label citizen observations matching expert occurrences
    matchGbifDatasets = PipelineStep( "matchGbifDatasets", func = simple_sql_query)

    #merge_inatOccurences = PipelineStep( "merge_inatOccurences", func = simple_sql_query
    
    submodule.add_step(inatOccMetadata)
    submodule.add_step(inatSpeciesData)

    #submodule.add_step(merge_inatOccurences)
    submodule.add_step(matchGbifDatasets)


    
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
    occurenceIDs = occurenceIDs[:1000]
    
    fields_string = f"({fields_to_string(fields)})"

    await query.run(pipe,step, occurenceIDs,'observations', fields_string )    

    
async def get_inatSpeciesData(pipe, step):
    
    #query = create_query('inat', name = 'inat_species' )
    
    pass