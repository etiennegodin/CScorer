from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, List, Dict, Any, Optional
from pprint import pprint
from enum import Enum
import logging 
import time
from pathlib import Path
from Cscorer.utils.yaml import yaml_serializable
import yaml

# Custom YAML handlers
def stepstatus_representer(dumper, data):
    return dumper.represent_scalar("!StepStatus", data.value)

def stepstatus_constructor(loader, node):
    value = loader.construct_scalar(node)
    return StepStatus(value)

def read_config(path:Path):
    import yaml
    path = to_Path(path)
    with open(path, 'r') as file:
        return yaml.load(file, Loader=yaml.FullLoader)

def to_Path(file_path: str):
    if isinstance(file_path, Path):
        return file_path
    if not isinstance(file_path, Path):
        return Path(file_path)


def write_config(config:dict, path:Path):
    yaml.add_representer(StepStatus, stepstatus_representer)
    path = to_Path(path)
    try:
        file=open(path,"w")
        yaml.dump(config,file)
        file.close() 
        print(path)
    except Exception as e:
        raise RuntimeError(e)
    
    return path

class StepStatus(str, Enum):
    init = "init"    
    requested = "requested" 
    pending = 'pending'
    ready = "ready"
    local = 'local'
    completed = "completed"
    failed = "failed"
    
@yaml_serializable()
@dataclass
class PipelineModule:
    name: str
    steps: Dict[str, PipelineStep] = field(default_factory=dict)
    status: StepStatus = StepStatus.init

@yaml_serializable()
@dataclass
class PipelineStep:
    name: str
    module :str
    substeps: Dict[str, PipelineSubstep] = field(default_factory=dict)
    status: StepStatus = StepStatus.init
    
@yaml_serializable()
@dataclass
class PipelineSubstep:
    name: str
    step:str
    module:str
    status: StepStatus = StepStatus.init
    output: Dict[str, Any] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)

@dataclass
class PipelineData:
    name:str
    modules: Dict[str, PipelineModule] = field(default_factory=dict)
    config: Dict[str, PipelineModule] = field(default_factory=dict)
    def add_module(self, name):
        module = PipelineModule(name)
        self.modules[name] = module
        return module
        
    def add_step(self, module:PipelineModule,step_name:str):
        if module.name in self.modules.keys():
            step = PipelineStep(step_name, module = module.name)
            self.modules[module.name].steps[step_name] = step
            return step
        
    def add_substep(self, step:PipelineStep,substep_name:str):
        if step.name in self.modules[step.module].steps.keys():
            substep = PipelineSubstep(substep_name, step = step.name, module = step.module)
            self.modules[step.module].steps[step.name].substeps[substep_name] = substep
            return substep
        

    
pipe = PipelineData('my_pipeline', config = {'yo':"sckos"})

data = pipe.add_module('data')
features = pipe.add_module('features')
loader = pipe.add_step(data, 'loader')
preprocessors = pipe.add_step(data, 'preprocessor')
gbif = pipe.add_substep(loader, 'gbif')
x = pipe.__dict__
#pprint(x)

write_config(x, 'test.yaml')
        
