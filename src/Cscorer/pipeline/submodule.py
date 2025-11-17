from __future__ import annotations
from typing import Callable, List, Dict, Any, Optional
from dataclasses import dataclass, field
import asyncio
from .yaml_support import yaml_serializable
from .core import Observable, init_data
from .enums import StepStatus

            
@yaml_serializable()
@dataclass
class PipelineSubmodule(Observable):
    name: str
    steps: Dict[str, PipelineStep] = field(default_factory=dict)
    status: StepStatus = StepStatus.init
    
    def __post_init__(self):
        self.data = init_data()
    
    def add_step(self, step:PipelineStep):
        step.set_parent(self)
        self.steps[step.name] = step

    async def run(self,pipe:Pipeline):
        incomplete_steps = []
        for step in self.steps.values():
            if step.status == StepStatus.init:
                incomplete_steps.append(step)
                
        submodule_tasks = [asyncio.create_task(step.run(pipe)) for step in incomplete_steps]
        print(submodule_tasks)
        await asyncio.gather(*submodule_tasks)
        
    def _child_updated(self, child, key, old, new):
        if self._parent:
            self._parent._child_updated(child, key, old, new)