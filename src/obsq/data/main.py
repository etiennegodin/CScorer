from .loaders.main import data_loaders
from .preprocessors.main import data_preprocessors

from ..pipeline import Pipeline, PipelineModule, PipelineSubmodule, StepStatus
import asyncio

async def data_submodules(pipe:Pipeline, module:PipelineModule): 
    pass