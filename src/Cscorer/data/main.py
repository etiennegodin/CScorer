from .loaders.main import set_loaders
from .preprocessors.main import preprocessors_main

from ..core import Pipeline, PipelineModule,PipelineStep
import asyncio

async def data_main(pipe:Pipeline):

    module = pipe.add_module(name ='data')
    await set_loaders(pipe,module)
    #await loaders.run_submodule())
    #asyncio.run(preprocessors_main(data))

    # 