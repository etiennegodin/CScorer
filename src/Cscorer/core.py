from typing import Callable, List, Dict, Any, Optional
from shapely import Point
from pathlib import Path
from dataclasses import dataclass, field
import logging
import pandas as pd 
import geopandas as gpd
import time
from enum import Enum

@dataclass
class PipelineSubstep:
    substep: str

@dataclass
class PipelineStep:
    step: list[PipelineStep] = field(default_factory=list)
    def add_substep(self,substep_name):
        self.substep = PipelineSubstep(substep= substep_name)

@dataclass
class PipelineModule:
    module: str
    steps: list[PipelineStep] = field(default_factory=list)
    substeps: list[PipelineSubstep] = field(default_factory=list)

    def add_step(self, step_name:str):
        self.steps.append(PipelineStep(step = step_name))
    def add_substep(self,substep_name:str):
        self.step.add_substep(substep_name)
        

class StepStatus(str, Enum):
    init = "init"    
    requested = "requested" 
    pending = 'pending'
    ready = "ready"
    local = 'local'
    completed = "completed"
    failed = "failed"

# Custom YAML handlers
def stepstatus_representer(dumper, data):
    return dumper.represent_scalar("!StepStatus", data.value)

def stepstatus_constructor(loader, node):
    value = loader.construct_scalar(node)
    return StepStatus(value)

@dataclass
class PipelineData:
    config: Dict[str, Any] = field(default_factory=dict)
    storage: Dict[str, Any] = field(default_factory=dict)
    step_status: Dict[str, Any] = field(default_factory=dict)
    logger: logging.Logger = None
    
    def __post_init__(self):

        #Init logger if empty
        if self.logger is None:
            self.logger = logging
            self.logger.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
        
        self.logger.info("Pipeline data post_init")
        
        #Init print
        self.init_new_step(step_name='Main')
        self.update_step_status('Main', StepStatus.init)
        

        # Init db_connection 
        from .utils.duckdb import _open_connection
        self.con = _open_connection(db_path=self.config['db_path'] )
        # Register handlers
        self._export()
        
    def init_new_module(self,module_name:str):
        
        self._write_to_step_status(module_name, level = 0)
        pass
    def init_new_substep(self,substep_name:str):
        pass
        
    def init_new_step(self, step_name:str, parent_step:str=None)-> bool:
        
        if parent_step is None:
            try:
                step_name
            
        
        if not step_name in self.storage.keys():
            self.logger.info(f"First time running {step_name}, creating storage and step status")
            self.set(step_name,  {'init' : time.time()})
            self.update_step_status(step_name, StepStatus.init)
            return True
        
        if self.step_status[step_name] == StepStatus.failed:
            #Retry
            self.update_step_status(step_name, StepStatus.init)

        
        return False
    
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
    
    
    def update_step_status(self,step:str, status: StepStatus):
        self.step_status[step] = status
        self._export()

    def get(self, key:str):
        return self.storage.get(key)
    
    def save_data(self, keys:str, value:Any):
        key_splits = keys.split(sep="_", maxsplit=3)
        module = key_splits[0]
        step = key_splits[1]
        substep = key_splits[2]
        
        
        
    
    def update(self):
        self._export()
    
    def set(self, key:str, value: Any):
        self.storage[key] = value
        self._export()
    
    def set_config(self, key:str, value: Any):
        self.config[key] = value
        self._export
            
    def _export(self):
        try:
            pipeline_folder = self.config['folders'].get('pipeline_folder')
            if pipeline_folder is not None:
                write_config(self.config, Path(pipeline_folder) / 'pipe_config.yaml')
                write_config(self.step_status, Path(pipeline_folder) / 'pipe_steps.yaml')
                write_config(self.storage, Path(pipeline_folder) / 'pipe_data.yaml')
                return True
            else:
                self.logger.error("No pipeline folder found in PipelineData")
                raise ValueError("No pipeline folder found in PipelineData")
            
        except Exception as e:
            # do not raise from storage persistence
            self.logger.error(f"Failed to persist pipeline storage : {e}") 
            
            return False
        
def read_config(path:Path):
    import yaml
    yaml.add_constructor("!StepStatus", stepstatus_constructor)

    path = to_Path(path)
    with open(path, 'r') as file:
        return yaml.load(file, Loader=yaml.FullLoader)
    
def write_config(config:dict, path:Path):
    import yaml
    yaml.add_representer(StepStatus, stepstatus_representer)

    path = to_Path(path)
    try:
        file=open(path,"w")
        yaml.dump(config,file)
        file.close() 
    except Exception as e:
        raise RuntimeError(e)
    
    return path

def to_Path(file_path: str):
    if isinstance(file_path, Path):
        return file_path
    if not isinstance(file_path, Path):
        return Path(file_path)

def convert_df_to_gdf(df : pd.DataFrame, lat_col : str = 'decimalLatitude', long_col : str = 'decimalLongitude', crs = 4326, verbose = False):
    return gpd.GeoDataFrame(df, geometry=[Point(xy) for xy in zip(df["decimalLongitude"], df["decimalLatitude"])] , crs = 4326 )

def rename_col_df(df:pd.DataFrame|gpd.GeoDataFrame, old:str = None, new:str = None):
    #Replace ":" with "_" in columns
    cols = df.columns.to_list()
    for col in cols:
        if old in col:
            df.rename(columns={col : str(col).replace(old,new)}, inplace= True)
    return df
    
