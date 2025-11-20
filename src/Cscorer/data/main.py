from .loaders.main import data_loaders
from .preprocessors.main import data_preprocessors

from ..pipeline import Pipeline, PipelineModule, PipelineSubmodule, StepStatus
import asyncio

async def data_submodules(pipe:Pipeline, module:PipelineModule): 
    loaders_submodule = PipelineSubmodule("loaders", func = data_loaders )
    preprocessors_submodule = PipelineSubmodule("preprocessors", func = data_preprocessors )

    module.add_submodule(loaders_submodule)
    module.add_submodule(preprocessors_submodule)
    
    
        
    #await loaders.run_submodule())
    #asyncio.run(preprocessors_main(data))

    # 