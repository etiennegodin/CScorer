from ..pipeline import Pipeline, PipelineModule, PipelineSubmodule, StepStatus
from .observer.observer import observer_features
from ..utils.duckdb import create_schema

async def features_submodules(pipe:Pipeline, module:PipelineModule):
    create_schema(pipe.con, "features")

    module.reset_submodules()
    observer_submodule = PipelineSubmodule("observer", func = observer_features )
    module.add_submodule(observer_submodule)

    