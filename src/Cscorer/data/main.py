from .loaders.main import loaders_main
from .preprocessors.main import data_preprocess_main

from ..core import Pipeline, PipelineModule
import asyncio


def data_main(pipe:Pipeline):
    print(pipe.logger)
    quit()
    module = pipe.add_module('data')
    asyncio.run(loaders_main(pipe, module))
    #asyncio.run(preprocessors_main(data))

    # 