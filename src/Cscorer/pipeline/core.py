from __future__ import annotations
from typing import Callable, List, Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass, field
import logging
import time
from enum import Enum
import yaml
import asyncio 
import importlib
import inspect
import duckdb
from ..utils.yaml import yaml_serializable


def load_function(path: str):
    print(path)
    print(type(path))
    mod_name, func_name = path.rsplit(".", 1)
    module = importlib.import_module(mod_name)
    return getattr(module, func_name)

#Initiliaze data for PipelineObject
def _init_data():
    return {'init': time.strftime("%Y-%m-%d %H:%M:%S") }

class StepStatus(str, Enum):
    init = "init"    
    requested = "requested" 
    pending = 'pending'
    ready = "ready"
    local = 'local'
    completed = "completed"
    failed = "failed"
    
    
class Observable:
    """Base class to track attribute changes and propagate updates up the parent hierarchy."""
    
    def __post_init__(self):
        # Called after dataclass __init__, safe place to initialize runtime fields
        self._parent = None

    def set_parent(self, parent):
        self._parent = parent

    def __setattr__(self, key, value):
        # For changes AFTER initialization starts
        if key != "_parent" and hasattr(self, "_parent"):
            old = self.__dict__.get(key, None)
            super().__setattr__(key, value)
            parent = self.__dict__.get("_parent")
            if parent:
                parent._child_updated(self, key, old, value)
        else:
            # Normal set during initial construction
            super().__setattr__(key, value)


    
@yaml_serializable()
@dataclass
class PipelineModule(Observable):
    name: str
    submodules: Dict[str, PipelineSubmodule] = field(default_factory=dict)
    status: StepStatus = StepStatus.init
    
    def __post_init__(self):
        self.data = _init_data()
        
    def add_submodule(self, submodule:PipelineSubmodule):
        submodule.set_parent(self)
        self.submodules[submodule.name] = submodule
        
    def _child_updated(self, child, key, old, new):
        if self._parent:
            self._parent._child_updated(child, key, old, new)
            
@yaml_serializable()
@dataclass
class PipelineSubmodule(Observable):
    name: str
    steps: Dict[str, PipelineStep] = field(default_factory=dict)
    status: StepStatus = StepStatus.init
    
    def __post_init__(self):
        self.data = _init_data()
    
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
        
@yaml_serializable()
@dataclass
class PipelineStep(Observable):
    name: str
    func: str
    storage: Dict[str, Any] = field(default_factory= dict)
    config: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        self.data = _init_data()
        self.status =  StepStatus.init

    async def run(self, pipe:Pipeline):
        func = self.func
        #func = load_function(self.func)
        if inspect.iscoroutinefunction(func):
            return await func(pipe, self)
        else:
            return func(pipe,self)
    
@yaml_serializable()
@dataclass
class Pipeline(Observable):
    modules: Dict[str, PipelineModule] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    logger: logging = logging
    con: duckdb.DuckDBPyConnection = None

    __yaml_exclude__ = {"con","logger", "_parent"}
    
    @classmethod
    def from_yaml_file(cls, path: str):
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return cls(**data)
    
    def __post_init__(self):
        #Init logger if empty
        self.logger.info("Pipeline data post_init")
        # Init db_connection 
        from ..utils.duckdb import _open_connection   
        self.con = _open_connection(db_path=self.config['db_path'] )
        # Register handlers
        self._export()

    def add_module(self, module:PipelineModule):
        module.set_parent(self)
        self.modules[module.name] = module
        self._export()
        
    def update(self):
        self._export()
        
    def _child_updated(self, child, key, old, new):
        self._export()

    def _export(self):
        path = Path(self.config['folders'].get('pipeline_folder')) / 'pipe.yaml'
        try:
            with open(path, "w") as f:
                yaml.dump(self, f)  # uses your yaml_serializable representer
            return True
        except Exception as e:
            # do not raise from storage persistence
            self.logger.error(f"Failed to persist pipeline storage : {e}") 
            return False
        
    def rebuild_runtime(self):
        for mod in self.modules.values():
            mod.set_parent(self)
            for sm in mod.submodules.values():
                sm.set_parent(mod)
                for step in sm.steps.values():
                    step.set_parent(sm)
