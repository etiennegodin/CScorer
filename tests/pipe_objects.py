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
from Cscorer.utils.yaml import yaml_serializable
from Cscorer.core import Pipeline, PipelineModule, PipelineStep, PipelineSubstep

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

    
pipe = Pipeline().from_yaml_file("")

data = pipe.add_module('data')
features = pipe.add_module('features')
loader = pipe.add_step(data, 'loader')
preprocessors = pipe.add_step(data, 'preprocessor')
gbif = pipe.add_substep(loader, 'gbif')
x = pipe.__dict__
#pprint(x)

write_config(x, 'test.yaml')
        
