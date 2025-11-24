from ...pipeline import Pipeline, PipelineSubmodule, PipelineStep, StepStatus
from ...utils.sql import simple_sql_query
import asyncio
from pathlib import Path


async def features_extractors(pipe:Pipeline, submodule:PipelineSubmodule):
    
    folder = Path(__file__).parent
    
    submodule.reset_steps()
    
    observer_features = PipelineStep( "observer_features", func = simple_sql_query)
    occurrence_features = PipelineStep( "occurrence_features", func = simple_sql_query)
    metadata_features = PipelineStep( "metadata_features", func = simple_sql_query)



    submodule.add_step(observer_features)
    await observer_features.run(pipe, sql_folder = folder)

    submodule.add_step(occurrence_features)
    await occurrence_features.run(pipe, sql_folder = folder)
    
    
    submodule.add_step(metadata_features)
    await metadata_features.run(pipe, sql_folder = folder)
    
\
    
    #tasks = [asyncio.create_task(step.run(pipe, sql_folder = folder)) for step in submodule.steps.values()]
    #await asyncio.gather(*tasks)
    
    
    
async def features_extractors_dfs(pipe:Pipeline, step:PipelineStep):
    import featuretools as ft
    con = pipe.con
    
    observations_df = con.execute("SELECT * FROM preprocessed.gbif_citizen").df()
    users_df = con.execute("SELECT * FROM preprocessed.inat_observers").df()
    env_df = con.execute("SELECT * FROM raw.gee_citizen_occurences").df()
    
    dataframes = { 'obs' : (observations_df, 'gbifID'),
                  'users' : (users_df, 'id'),
                  'env' : (env_df, 'gbifID')
    }
    
    relationships = [ ("obs", 'recordedBy', 'users', 'name'),
                    ('obs', 'gbifID', 'env', 'gbifID'),
    ]
    
    feature_matrix_customers, features_defs = ft.dfs(
    dataframes=dataframes,
    relationships=relationships,
    target_dataframe_name="customers",
    )
    
    

    