from ...pipeline import Pipeline, PipelineModule, PipelineSubmodule,PipelineStep, StepStatus
from ...utils.duckdb import create_schema
from ...utils.sql import read_sql_template, read_sql_file, simple_sql_query
import duckdb
import asyncio
from pathlib import Path
from ..loaders.factory import create_query
from ..loaders.inat import fields_to_string
from ..loaders.gee import upload_points
from pprint import pprint

async def data_post_loaders(pipe:Pipeline, submodule:PipelineSubmodule):

    sql_folder = Path(__file__).parent
    submodule.reset_steps()

    submodule.add_step(PipelineStep("data_load_inat_observer", func = data_load_inat_observer))
    submodule.add_step(PipelineStep("data_load_points", func = data_load_points))

    #Query iNat API with reduced gbif citizen occurences 
    inatObservations = PipelineStep("data_post_load_inatObservations", func = get_inatOccMetadata)
    
    #Query species phenology
    species_phenology = PipelineStep("data_post_load_speciesPhenology", func = get_species_phenology)
    
    #Query similar species  
    similar_species = PipelineStep("data_post_load_similarSpecies", func = get_similar_species)

    # Store ranges from gpkg 
    

    # Label citizen observations matching expert occurrences
    matchGbifDatasets = PipelineStep( "matchGbifDatasets", func = simple_sql_query)

    #merge_inatOccurences = PipelineStep( "merge_inatOccurences", func = simple_sql_query
    
    submodule.add_step(inatObservations)
    submodule.add_step(species_phenology)
    submodule.add_step(similar_species)

    #submodule.add_step(merge_inatOccurences)
    submodule.add_step(matchGbifDatasets)
    
    #await inatObservations.run(pipe)
    #await species_phenology.run(pipe)
    #await similar_species.run(pipe)


    #await matchGbifDatasets.run(pipe, sql_folder = sql_folder)
    
    
    async with asyncio.TaskGroup() as tg:
        tg.create_task(submodule.steps["data_load_inat_observer"].run(pipe))
        gee_upload_task = tg.create_task(submodule.steps["data_load_points"].run(pipe))


    points_dict = submodule.steps["data_load_points"].storage['points']

    for name, points in points_dict.items():
        submodule.add_step(PipelineStep(f"data_load_sample_gee_{name}", func = data_load_sample_gee))
        await submodule.steps[f"data_load_sample_gee_{name}"].run(pipe, points = points)


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
            ORDER BY occurrenceID ASC;

    """
    occurenceURLs = pipe.con.execute(get_occurencesIDs).df()[key]
    occurenceIDs = occurenceURLs.apply(lambda x: x.split(sep='/')[-1]).to_list()
    occurenceIDs = occurenceIDs[:1000]
    
    fields_string = f"({fields_to_string(fields)})"

    await query.run(pipe,step, occurenceIDs,'observations', fields_string )    

    
async def get_species_phenology(pipe, step):
    fields = {
        "count" : True,
        "controlled_attribute" : {"id":True, "label":True},
        "controlled_value" : {"id":True, "label":True},
        "month_of_year" : True

    }
    
    key = "occurrenceID"
    get_occurencesIDs = f"""
            SELECT {key},
            FROM preprocessed.gbif_citizen_filtered
            ORDER BY occurrenceID ASC;

    """
    
    query = create_query('inat', pipe, name = 'inat_phenology' )

    
    pass

    
async def get_similar_species(pipe, step):
    
    #query = create_query('inat', name = 'inat_species' )
    
    pass


async def data_load_inat_observer(pipe:Pipeline, step:PipelineStep):
    con = pipe.con
    # Create query 
    inatObs_query = create_query('inatObs', name = step.name)
    #Init step
    #Return url for 
    oberver_table = await inatObs_query.run(pipe,step)    

async def data_load_points(pipe:Pipeline, step:PipelineStep):
    #Create table schema on db 
    points_list = await upload_points(pipe,step)
    return points_list
    
async def data_load_sample_gee(pipe:Pipeline, step:PipelineStep, points:str):
    gee_query = create_query('gee', pipe, name = step.name, points=points)
    #Create list of queries (one for each set of occurences)
    await gee_query.run(pipe, step)