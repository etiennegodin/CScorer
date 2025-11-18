from __future__ import annotations
from dataclasses import dataclass, field
import time
import importlib

def check_completion(items:dict):
    from .enums import StepStatus
    completion = True
    for item in items.values():
        if item.status != StepStatus.completed:
                completion = False
    return completion

def load_function(path: str):
    print(path)
    print(type(path))
    mod_name, func_name = path.rsplit(".", 1)
    module = importlib.import_module(mod_name)
    return getattr(module, func_name)

#Initiliaze data for PipelineObject
def init_data():
    return {'init': time.strftime("%Y-%m-%d %H:%M:%S") }

class Observable:
    """Base class to track attribute changes and propagate updates up the parent hierarchy."""
    __yaml_exclude__ = {"_parent"}

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

