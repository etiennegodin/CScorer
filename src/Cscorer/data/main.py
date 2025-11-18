from .loaders.main import data_loaders_steps
from .preprocessors.main import preprocessors_main

from ..pipeline import Pipeline, PipelineModule, PipelineSubmodule, StepStatus
import asyncio

async def data_submodules(pipe:Pipeline, module:PipelineModule): 
    loaders_submodule = PipelineSubmodule("loaders", func = data_loaders_steps )
    #preprocessors_submodule = PipelineSubmodule("preprocessors")

    module.add_submodule(loaders_submodule)
    #module.add_submodule(preprocessors_submodule)
    
    
        
    #await loaders.run_submodule())
    #asyncio.run(preprocessors_main(data))

    # 