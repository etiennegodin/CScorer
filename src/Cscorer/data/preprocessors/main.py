from ...pipeline import Pipeline, PipelineModule, PipelineSubmodule,PipelineStep, StepStatus


async def data_preprocessors(pipe:Pipeline, submodule:PipelineSubmodule):
    submodule.add_step(PipelineStep( "data_prepro_dumy", func = dummy))
    print('yes')
    pass
    #data_preprocess_x
    #data_preprocess_y
    
    
async def dummy(pipe:Pipeline, step:PipelineStep):
    
    print('dummy')



