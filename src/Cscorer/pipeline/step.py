from __future__ import annotations
from typing import Callable, List, Dict, Any, Optional
from dataclasses import dataclass, field
import inspect
from .yaml_support import yaml_serializable
from .core import Observable, init_data
import time

@yaml_serializable()
@dataclass
class PipelineStep(Observable):
    from .enums import StepStatus
    name: str
    func: str
    storage: Dict[str, Any] = field(default_factory= dict)
    config: Dict[str, Any] = field(default_factory=dict)
    status: StepStatus = StepStatus.init
    timestamp:str = time.strftime("%Y-%m-%d %H:%M:%S")
    

    async def run(self, pipe:Pipeline):
        func = self.func
        #func = load_function(self.func)
        if inspect.iscoroutinefunction(func):
            return await func(pipe, self)
        else:
            return func(pipe,self)
    