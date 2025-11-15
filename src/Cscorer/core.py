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

@yaml_serializable
@dataclass
class PipelineModule:
    name: str
    steps: Dict[str, PipelineStep] = field(default_factory=dict)
    status: StepStatus = StepStatus.init
    data: Dict[str, Any] = field(default_factory=_init_data())

@yaml_serializable
@dataclass
class PipelineStep:
    name: str
    module :str
    substeps: Dict[str, PipelineSubstep] = field(default_factory=dict)
    status: StepStatus = StepStatus.init
    data: Dict[str, Any] = field(default_factory=_init_data())

@yaml_serializable
@dataclass
class PipelineSubstep:
    name: str
    step:str
    module:str
    status: StepStatus = StepStatus.init
    func: Callable = None
    data: Dict[str, Any] = field(default_factory=_init_data())
    config: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Pipeline:
    modules: Dict[str, PipelineModule] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    logger: logging.Logger = None
    
    @classmethod
    def from_yaml_file(cls, path: str):
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return cls(**data)
    
    def __post_init__(self):

        #Init logger if empty
        if self.logger is None:
            self.logger = logging
            self.logger.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
        
        self.logger.info("Pipeline data post_init")

        # Init db_connection 
        from .utils.duckdb import _open_connection
        self.con = _open_connection(db_path=self.config['db_path'] )
        # Register handlers
        self._export()

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
    
    def _write_to_step_status(self, step_name:str, level:int):
        step_splits = step_name.split(sep="_", maxsplit=3)
        try:module = step_splits[0]
        except:module = None
        try:step = step_splits[1]
        except: step = None
        try:substep = step_splits[2]
        except:substep = None
        
        if module is None:
            raise ValueError('Module not provided for step name. Please user {module}_{step}_{substep}')
        if step is None:
            if module not in self.storage.keys():
                self.logger.info(f"First time running module {module}, creating storage and step status")
                self.set(step_name,  {'init' : time.strftime("%Y-%m-%d %H:%M:%S")})
                return True
        if substep is None:
            if step not in self.storage[module].keys():
                self.logger.info(f"First time running step {module}_{step}, creating storage and step status")
                self.set(step_name,  {'init' : time.strftime("%Y-%m-%d %H:%M:%S")})
                return True
                    
        if not substep in self.storage[module][step].keys():
            self.logger.info(f"First time running substep {module}_{step}_{substep}, creating storage and step status")

            self.set(step_name,  {'init' : time.strftime("%Y-%m-%d %H:%M:%S")})
            self.update_step_status(step_name, StepStatus.init)
            return True

        raise ValueError('Step provided not written to pipeline data.\nPlease user {module}_{step}_{substep} format')
        return False
    
    def _export(self):
        try:
            pipeline_folder = self.config['folders'].get('pipeline_folder')
            out = self.__dict__
            out.pop('con')
            out.pop('logger')

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
    
