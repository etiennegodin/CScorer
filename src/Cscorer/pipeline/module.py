from __future__ import annotations
from typing import Callable, List, Dict, Any, Optional
from dataclasses import dataclass, field

from ..utils.yaml import yaml_serializable
from .core import Observable, init_data

from .submodule import PipelineSubmodule

from .enums import StepStatus



@yaml_serializable()
@dataclass
class PipelineModule(Observable):
    name: str
    submodules: Dict[str, PipelineSubmodule] = field(default_factory=dict)
    status: StepStatus = StepStatus.init
    
    def __post_init__(self):
        self.data = init_data()
        
    def add_submodule(self, submodule:PipelineSubmodule):
        submodule.set_parent(self)
        self.submodules[submodule.name] = submodule
        
    def _child_updated(self, child, key, old, new):
        if self._parent:
            self._parent._child_updated(child, key, old, new)