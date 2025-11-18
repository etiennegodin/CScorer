from __future__ import annotations
from typing import Callable, List, Dict, Any, Optional
from dataclasses import dataclass, field
import inspect
from .yaml_support import yaml_serializable
from .core import Observable, init_data
from .submodule import PipelineSubmodule
from .enums import StepStatus
import time


@yaml_serializable()
@dataclass
class PipelineModule(Observable):
    name: str
    func: str
    submodules: Dict[str, PipelineSubmodule] = field(default_factory=dict)
    status: StepStatus = StepStatus.init
    init = time.strftime("%Y-%m-%d %H:%M:%S")
        
    def add_submodule(self, submodule:PipelineSubmodule):
        if submodule.name not in self.submodules.keys():
            submodule.set_parent(self)
            self.submodules[submodule.name] = submodule
        
    def _child_updated(self, child, key, old, new):
        if self._parent:
            self._parent._child_updated(child, key, old, new)
            
    async def run(self):
        pipe = self._parent
        pipe.logger.info(f'Running module : {self.name}')
        func = self.func
        #func = load_function(self.func)
        if inspect.iscoroutinefunction(func):
            await func(pipe, self)
        else:
            func(pipe,self)
        
        for sm in self.submodules.values():
            if sm.status != StepStatus.completed:
                await sm.run(pipe)
            else:
                pipe.logger.info(f"{sm.name} submodule is completed")
                
        self.status = StepStatus.completed