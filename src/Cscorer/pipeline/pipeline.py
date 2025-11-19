from __future__ import annotations
from typing import Callable, List, Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass, field
import logging
import time
import yaml
import duckdb
from .yaml_support import yaml_serializable
from .core import Observable, check_completion
from .module import PipelineModule

@yaml_serializable()
@dataclass
class Pipeline(Observable):
    modules: Dict[str, PipelineModule] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    logger: logging = logging
    con: duckdb.DuckDBPyConnection = None
    init:str = time.strftime("%Y-%m-%d %H:%M:%S")

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
        if module.name not in self.modules.keys():
            module.set_parent(self)
            self.modules[module.name] = module
            self._export()
            
    def remove_module(self, module:PipelineModule):
        if module.name in self.modules.keys():
            self.modules.pop(module.name)
            self.logger.info(f"Reseted module {module.name}")
        else:
            self.logger.warning(f"Erro removing ,odule {module.name} from pipeline - Not found in pipeline's modules")

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
    
    async def run(self):
        from .enums import StepStatus
        for m in self.modules.values():
            if m.status == StepStatus.incomplete:
                continue
            for sm in m.submodules.values():
                if sm.status == StepStatus.incomplete:
                    continue
                for st in sm.steps.values():
                    if st.status != StepStatus.completed:
                        sm.status = StepStatus.incomplete
                        m.status = StepStatus.incomplete
        
        for m in self.modules.values():
            if m.status != StepStatus.completed:
                await m.run()
            else:
                self.logger.info(f"{m.name} module is completed")
            
    
    def rebuild_runtime(self):
        for mod in self.modules.values():
            mod.set_parent(self)
            for sm in mod.submodules.values():
                sm.set_parent(mod)
                for step in sm.steps.values():
                    step.set_parent(sm)
