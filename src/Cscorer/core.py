from __future__ import annotations
from typing import Callable, List, Dict, Any, Optional
from shapely import Point
from pathlib import Path
from dataclasses import dataclass, field
import logging
import pandas as pd 
import geopandas as gpd
import time
from enum import Enum
import yaml
from .utils.yaml import yaml_serializable
import asyncio 
import importlib
import inspect


def load_function(path: str):
    mod_name, func_name = path.rsplit(".", 1)
    module = importlib.import_module(mod_name)
    return getattr(module, func_name)

#Initiliaze data for PipelineObject
def _init_data():
    return {'init': time.strftime("%Y-%m-%d %H:%M:%S") }

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
    except Exception as e:
        raise RuntimeError(e)
    
    return path

@dataclass
class Observable:
    _callback: callable = field(default=None, init=False, repr=False)

    def set_callback(self, fn):
        self._callback = fn

    def __setattr__(self, key, value):
        old = self.__dict__.get(key, None)

        super().__setattr__(key, value)

        cb = self.__dict__.get("_callback", None)
        if cb and key != "_callback":
            cb(self, key, old, value)


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
    submodules: Dict[str, PipelineSubmodule] = field(default_factory=dict)
    status: StepStatus = StepStatus.init
    
    def __post_init__(self):
        self.data = _init_data()

    
@yaml_serializable()
@dataclass
class PipelineSubmodule:
    name: str
    module :str
    steps: Dict[str, PipelineStep] = field(default_factory=dict)
    status: StepStatus = StepStatus.init
    
    def __post_init__(self):
        self.data = _init_data()
    
    async def run(self,pipe:Pipeline):
        incomplete_steps = []
        for step in self.steps.values():
            if step.status == StepStatus.init:
                incomplete_steps.append(step)
                
        submodule_tasks = [asyncio.create_task(step.run(pipe, step)) for step in incomplete_steps]
        print(submodule_tasks)
        await asyncio.gather(*submodule_tasks)
        
@yaml_serializable()
@dataclass
class PipelineStep:
    name: str
    submodule:str
    module:str
    func_path: str
    status: StepStatus = StepStatus.init
    storage: Dict[str, Any] = field(default_factory= dict)
    config: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        self.data = _init_data()
        
        
    
    async def run(self, pipe:Pipeline, *args, **kwargs):
        func = load_function(self.func_path)
        if inspect.iscoroutinefunction(func):
            return await func(pipe, self, *args,**kwargs)
        else:
            return func(pipe,self,*args, **kwargs)
    
@yaml_serializable()
@dataclass
class Pipeline:
    modules: Dict[str, PipelineModule] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    logger: logging = logging
    
    @classmethod
    def from_yaml_file(cls, path: str):
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return cls(**data)
    
    def __post_init__(self):
        #Init logger if empty
        
        self.logger.info("Pipeline data post_init")
        # Init db_connection 
        from .utils.duckdb import _open_connection
        self.con = _open_connection(db_path=self.config['db_path'] )
        # Register handlers
        self._export()

    def add_module(self, *args, **kwargs):
        module = PipelineModule(*args,**kwargs)
        self.modules[module.name] = module
        self._export()
        return module
        
    def add_submodule(self, module:PipelineModule, submodule_name:str):
        if module.name in self.modules.keys():
            step = PipelineSubmodule(submodule_name, module = module.name)
            self.modules[module.name].submodules[submodule_name] = step
            self._export()
            return step

    def add_step(self, submodule:PipelineSubmodule,step_name:str,func:Callable):
        if submodule.name in self.modules[submodule.module].submodules.keys():
            path = f"{func.__module__}.{func.__name__}"
            step = PipelineStep(step_name, submodule = submodule.name, module = submodule.module, func_path = path)
            self.modules[submodule.module].submodules[submodule.name].steps[step_name] = step
            self._export()

            return step
        
    def update(self):
        self._export()
        
    
    def _export(self):
        try:
            pipeline_folder = self.config['folders'].get('pipeline_folder')
            export = self.__dict__
            #pprint(out)
            if "con" in export.keys():
                del export['con']
            if "logger" in export.keys():
                del export['logger']

            write_config(self.__dict__, Path(pipeline_folder) / 'pipe.yaml')
            return True
        except Exception as e:
            # do not raise from storage persistence
            self.logger.error(f"Failed to persist pipeline storage : {e}") 
            return False
        

def convert_df_to_gdf(df : pd.DataFrame, lat_col : str = 'decimalLatitude', long_col : str = 'decimalLongitude', crs = 4326, verbose = False):
    return gpd.GeoDataFrame(df, geometry=[Point(xy) for xy in zip(df["decimalLongitude"], df["decimalLatitude"])] , crs = 4326 )

def rename_col_df(df:pd.DataFrame|gpd.GeoDataFrame, old:str = None, new:str = None):
    #Replace ":" with "_" in columns
    cols = df.columns.to_list()
    for col in cols:
        if old in col:
            df.rename(columns={col : str(col).replace(old,new)}, inplace= True)
    return df
    
