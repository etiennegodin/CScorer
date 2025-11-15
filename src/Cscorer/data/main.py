from .loaders.main import data_load_main
from .preprocessors.main import data_preprocess_main

from ..core import PipelineData
import asyncio


def main(data:PipelineData):
    step_name = "data_main"
    data.init_new_step(step_name)
    
    asyncio.run(data_load_main(data))
    #asyncio.run(data_preprocess_main(data))

    # 