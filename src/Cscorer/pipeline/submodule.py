from __future__ import annotations
from typing import Callable, List, Dict, Any, Optional
from dataclasses import dataclass, field
import inspect
from .yaml_support import yaml_serializable
from .core import Observable, init_data
from .enums import StepStatus
import time
            
@yaml_serializable()
@dataclass
class PipelineSubmodule(Observable):
    name: str
    func: str
    steps: Dict[str, PipelineStep] = field(default_factory=dict)
    status: StepStatus = StepStatus.init
    init:str = time.strftime("%Y-%m-%d %H:%M:%S")
    
    def add_step(self, step:PipelineStep):
        if step.name not in self.steps.keys():
            step.set_parent(self)
            self.steps[step.name] = step

    async def run(self,pipe:Pipeline):
        pipe.logger.info(f'Running submodule : {self.name}')
        func = self.func
        #func = load_function(self.func)
        if inspect.iscoroutinefunction(func):
            await func(pipe, self)
            self.status = StepStatus.incomplete
        else:
            func(pipe,self)
            self.status = StepStatus.incomplete

        
        for s in self.steps.values():
            print(s)
            if s.status != StepStatus.completed:
                break
            
        #self.status = StepStatus.completed
           

        
    def _child_updated(self, child, key, old, new):
        if self._parent:
            self._parent._child_updated(child, key, old, new)