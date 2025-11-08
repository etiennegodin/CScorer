from typing import Callable, List, Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass, field
import logging
import yaml
import time
from enum import Enum

class StepStatus(str, Enum):
    init = "init"    
    requested = "requested" 
    ready = "ready"
    completed = "completed"
    pending = 'pending'
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
        self.set("init", time.time())
        self.update_step_status('init', StepStatus.init)
        # Register handlers
        self._export()
        
    def update_step_status(self,step:str, status: StepStatus):
        self.step_status[step] = status
        self._export()

    def get(self, key:str):
        return self.storage.get(key)
    
    def set(self, key:str, value: Any):
        self.storage[key] = value
        self._export()
    
    def set_config(self, key:str, value: Any):
        self.config[key] = value
        self._export
            
    def _export(self):
        self.logger.info("_export")

        try:
            data_folder = self.config.get('data_folder')
            if data_folder is not None:
                write_config(self.config, Path(data_folder) / 'pipe_config.yaml')
                write_config(self.step_status, Path(data_folder) / 'pipe_steps.yaml')
                write_config(self.storage, Path(data_folder) / 'pipe_data.yaml')
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
    
