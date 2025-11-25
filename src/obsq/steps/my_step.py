from obsq.pipeline import FunctionStep, step, PipelineContext

@step
def step1(context:PipelineContext):
    print('1')
    
@step
def step2(context:PipelineContext):
    print(2)