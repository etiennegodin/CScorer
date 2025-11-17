from __future__ import annotations
from typing import Callable, List, Dict, Any, Optional
from dataclasses import dataclass, field
import inspect
from .yaml_support import yaml_serializable
from .core import Observable, init_data

@yaml_serializable()
@dataclass
class PipelineStep(Observable):
    name: str
    func: str
    storage: Dict[str, Any] = field(default_factory= dict)
    config: Dict[str, Any] = field(default_factory=dict)
    
    from .pipeline import Pipeline
    
    def __post_init__(self):
        from .enums import StepStatus
        self.data = init_data()
        self.status =  StepStatus.init

    async def run(self, pipe:Pipeline):
        func = self.func
        #func = load_function(self.func)
        if inspect.iscoroutinefunction(func):
            return await func(pipe, self)
        else:
            return func(pipe,self)
    