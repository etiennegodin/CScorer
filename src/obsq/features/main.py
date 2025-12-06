from ..pipeline import Pipeline, PipelineModule, PipelineSubmodule, StepStatus
from ..utils.duckdb import create_schema

async def features_submodules(pipe:Pipeline, module:PipelineModule):
    
    create_schema(pipe.con, "features")
    print('prut')
    