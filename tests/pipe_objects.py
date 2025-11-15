from dataclasses import dataclass, field
from typing import Callable, List, Dict, Any, Optional
from pprint import pprint
from enum import Enum


class StepStatus(str, Enum):
    init = "init"    
    requested = "requested" 
    pending = 'pending'
    ready = "ready"
    local = 'local'
    completed = "completed"
    failed = "failed"





@dataclass
class PipelineSubstep:
    substep: str

@dataclass
class PipelineStep:
    name: str
    substeps: Dict[str, PipelineSubstep] = field(default_factory=dict)

    def add_substep(self,substep_name):
        self.substep = PipelineSubstep(substep= substep_name)
        return self.substep


@dataclass
class PipelineModule:
    name: str
    steps: Dict[str, PipelineStep] = field(default_factory=dict)


@dataclass
class PipelineSubstep:
    name: str
    parent:str
    

@dataclass
class PipelineStep:
    name: str
    parent :str
    substeps: Dict[str, PipelineSubstep] = field(default_factory=dict)

    def add_substep(self,substep_name):
        self.substep = PipelineSubstep(substep= substep_name)
        return self.substep

@dataclass
class PipelineData:
    name:str
    modules: Dict[str, PipelineModule] = field(default_factory=dict)
    
    def add_module(self, name):
        module = PipelineModule(name)
        self.modules[name] = module
        return module
        
    def add_step(self, module:PipelineModule,step_name:str):
        if module.name in self.modules.keys():
            step = PipelineStep(step_name, parent = module.name)
            self.modules[module.name].steps[step_name] = step
            return step
        
    def add_substep(self, step:PipelineStep,substep_name:str):
        if step.name in self.modules[step.parent].steps.keys():
            substep = PipelineSubstep(substep_name, parent = step.name)
            self.modules[step.parent].steps[step.name].substeps[substep_name] = substep
            return substep
        

    
pipe = PipelineData('my_pipeline')

data = pipe.add_module('data')
loader = pipe.add_step(data, 'loader')
gbif = pipe.add_substep(loader, 'gbif')
pipe.add_substep(loader, 'gbif2')
pprint(pipe.__dict__)
pprint(data.__dict__)
pprint(gbif.__dict__)


