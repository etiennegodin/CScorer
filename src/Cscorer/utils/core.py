from typing import Callable, List, Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass, field
import logging
import yaml

class ProjectConfig:
    config: Dict[str, Any] = field(default_factory=dict)
    storage: Dict[str, Any] = field(default_factory=dict)
    
    def get(self, key:str):
        return self.storage.get(key)
    
    def set(self, key:str, value: Any):
        self.storage[key] = value
        self._export_storage()
    
    def _export_storage(self):
        try:
            run_folder = self.config.get('run_folder')
            if run_folder:
                write_config(self.storage, Path(run_folder) / 'pipe_data.yaml')
                return True
        except Exception:
            # do not raise from storage persistence
            logging.getLogger(__name__).exception("Failed to persist pipeline storage") 
            return False
        
def read_config(path:Path):
    path = to_Path(path)
    import yaml
    with open(path, 'r') as file:
        return yaml.safe_load(file)
    
def write_config(config:dict, path:Path):
    path = to_Path(path)
    try:
        file=open(path,"w")
        yaml.dump(config,file)
        file.close()
        
    except Exception as e:
        raise e
    
    return path

def to_Path(file_path: str):
    if isinstance(file_path, Path):
        return file_path
    if not isinstance(file_path, Path):
        return Path(file_path)