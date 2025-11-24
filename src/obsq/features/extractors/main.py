from ...pipeline import Pipeline, PipelineSubmodule, PipelineStep, StepStatus
from ...utils.sql import simple_sql_query
import asyncio
from pathlib import Path


async def features_extractors(pipe:Pipeline, submodule:PipelineSubmodule):
    
    folder = Path(__file__).parent
    
    submodule.reset_steps()
    
    observer_features = PipelineStep( "all_observer_features", func = simple_sql_query)

    submodule.add_step(observer_features)
    await observer_features.run(pipe, sql_folder = folder)


    #tasks = [asyncio.create_task(step.run(pipe, sql_folder = folder)) for step in submodule.steps.values()]
    #await asyncio.gather(*tasks)
    
    
    
async def test(pipe:Pipeline, step:PipelineStep):
    print('test')
