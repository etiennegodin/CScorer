from ...pipeline import Pipeline, PipelineSubmodule, PipelineStep, StepStatus
from ...utils.sql import simple_sql_query
import asyncio
from pathlib import Path

"""
# For each observer, calculate:
- total_observations (experience proxy)
- species_diversity (specialist vs generalist)
- observation_frequency (active vs casual)
- expert_agreements_received (quality track record)
- average_community_validation_time
- observation_detail_length (metadata richness)
"""

async def features_observer(pipe:Pipeline, submodule:PipelineSubmodule):
    sql_folder = Path(__file__).parent
    submodule.reset_steps()
    observer = PipelineStep( "observer", func = simple_sql_query)

    submodule.add_step(observer)
    
    tasks = [asyncio.create_task(step.run(pipe, sql_folder = sql_folder)) for step in submodule.steps.values()]
    await asyncio.gather(*tasks)
    
    
    
async def test(pipe:Pipeline, step:PipelineStep):
    print('test')
