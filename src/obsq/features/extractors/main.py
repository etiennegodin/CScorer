from ...pipeline import Pipeline, PipelineSubmodule, PipelineStep, StepStatus
from ...utils.sql import simple_sql_query
import asyncio
from pathlib import Path


async def features_extractors(pipe:Pipeline, submodule:PipelineSubmodule):
    
    folder = Path(__file__).parent
    
    submodule.reset_steps()
    
    observer_features = PipelineStep( "observer_features", func = simple_sql_query)
    extract_inat_expert = PipelineStep( "extract_inat_expert", func = simple_sql_query)
    shuffle_inat_expert_obs = PipelineStep( "shuffle_inat_expert_obs", func = simple_sql_query)

    submodule.add_step(observer_features)
    submodule.add_step(extract_inat_expert)
    submodule.add_step(shuffle_inat_expert_obs)

    await observer_features.run(pipe, sql_folder = folder)
    await extract_inat_expert.run(pipe, sql_folder = folder)
    await shuffle_inat_expert_obs.run(pipe, sql_folder = folder)

    #tasks = [asyncio.create_task(step.run(pipe, sql_folder = folder)) for step in submodule.steps.values()]
    #await asyncio.gather(*tasks)
    
    
    
async def test(pipe:Pipeline, step:PipelineStep):
    print('test')
