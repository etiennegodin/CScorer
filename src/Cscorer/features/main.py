from ..pipeline import Pipeline, PipelineModule, PipelineSubmodule, StepStatus
from .observer import observer_features

async def features_submodules(pipe:Pipeline, module:PipelineModule):
    observer_submodule = PipelineSubmodule("observer", func = observer_features )
    module.add_submodule(observer_submodule)

    