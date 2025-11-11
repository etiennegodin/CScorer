from typing import Callable, List, Dict, Any, Optional
from shapely import Point
from pathlib import Path
from dataclasses import dataclass, field
import logging
import pandas as pd 
import geopandas as gpd
import time
from enum import Enum

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
    logger: logging.Logger = logging
    
    def __post_init__(self):
        self.logger.info("Pipeline data post_init")
        #Init print
        self.set("init", time.time())
        self.update_step_status('init', StepStatus.init)
        
        # Init db_connection 
        from .utils.duckdb import _open_connection
        db_path = f"{self.config['folders']['data_folder']}/data.duckdb"
        self.con = _open_connection(db_path=db_path )
        self.config['db_path'] = db_path
        # Register handlers
        self._export()
        
    def update_step_status(self,step:str, status: StepStatus):
        self.step_status[step] = status
        self._export()

    def get(self, key:str):
        return self.storage.get(key)
    
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
            data_folder = self.config['folders'].get('data_folder')
            if data_folder is not None:
                write_config(self.config, Path(data_folder) / 'pipeline' / 'pipe_config.yaml')
                write_config(self.step_status, Path(data_folder) / 'pipeline' / 'pipe_steps.yaml')
                write_config(self.storage, Path(data_folder) / 'pipeline' / 'pipe_data.yaml')
                return True
            else:
                self.logger.error("No data folder found in PipelineData")
                raise ValueError("No data folder found in PipelineData")
            
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
    
