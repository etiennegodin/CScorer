from ..pipeline import Pipeline, PipelineModule, PipelineSubmodule, StepStatus
from .observer.main import features_observer
from ..utils.duckdb import create_schema

async def features_submodules(pipe:Pipeline, module:PipelineModule):
    create_schema(pipe.con, "features")

    observer_submodule = PipelineSubmodule("observer", func = features_observer )
    module.add_submodule(observer_submodule)

    