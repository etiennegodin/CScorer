from enum import Enum
import yaml


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
    from ..pipeline import StepStatus
    value = loader.construct_scalar(node)
    return StepStatus(value)


# register immediately on import
yaml.add_representer(StepStatus, stepstatus_representer)
yaml.add_constructor("!StepStatus", stepstatus_constructor)
    