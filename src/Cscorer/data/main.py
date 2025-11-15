from .loaders.main import data_load_main
from .preprocessors.main import data_preprocess_main

from ..core import Pipeline, PipelineModule
import asyncio


def main(pipe:Pipeline):
    data = pipe.add_module('data')
    loader = pipe.add_step(data, 'loaders')
    asyncio.run(data_load_main(pipe, data))
    #asyncio.run(data_preprocess_main(data))

    # 