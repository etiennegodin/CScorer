from .loaders.main import set_loaders
from .preprocessors.main import preprocessors_main

from ..core import Pipeline, PipelineModule,PipelineStep
import asyncio

async def data_main(pipe:Pipeline):
    data_module = PipelineModule('data')
    pipe.add_module(data_module)
    
    await set_loaders(pipe,data_module)
    #await loaders.run_submodule())
    #asyncio.run(preprocessors_main(data))

    # 