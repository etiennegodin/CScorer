from ..pipeline import Pipeline, PipelineSubmodule, PipelineStep, StepStatus
from ..utils.sql import simple_sql_query
import asyncio


"""
# For each observer, calculate:
- total_observations (experience proxy)
- species_diversity (specialist vs generalist)
- observation_frequency (active vs casual)
- expert_agreements_received (quality track record)
- average_community_validation_time
- observation_detail_length (metadata richness)


"""

async def observer_features(pipe:Pipeline, submodule:PipelineSubmodule):
    #create_schema(pipe.con, schema)
    
    total_observations = PipelineStep( "total_observations", func = test)

    submodule.add_step(total_observations)
    
    tasks = [asyncio.create_task(step.run(pipe)) for step in submodule.steps.values()]
    await asyncio.gather(*tasks)
    
async def test(pipe:Pipeline, step:PipelineStep):
    print('test')
