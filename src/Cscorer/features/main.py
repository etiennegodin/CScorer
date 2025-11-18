from ..pipeline import Pipeline, PipelineModule, PipelineSubmodule, StepStatus


async def features_submodules(pipe:Pipeline, module:PipelineModule):
    
    """
    loaders_submodule = PipelineSubmodule("loaders", func = data_loaders_steps )
    preprocessors_submodule = PipelineSubmodule("preprocessors", func =data_preprocessors )

    module.add_submodule(loaders_submodule)
    module.add_submodule(preprocessors_submodule)
    """