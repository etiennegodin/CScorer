
import yaml
from pathlib import Path
CLASS_REGISTRY = {}

# Custom YAML handlers
def stepstatus_representer(dumper, data):
    return dumper.represent_scalar("!StepStatus", data.value)

def stepstatus_constructor(loader, node):
    from ..core import StepStatus
    value = loader.construct_scalar(node)
    return StepStatus(value)


def yaml_serializable(tag=None):
    from ..core import StepStatus

    def wrapper(cls):

        yaml_tag = tag or f"!{cls.__name__}"
        CLASS_REGISTRY[yaml_tag] = cls

        def representer(dumper, obj):
            exclude = getattr(obj, "__yaml_exclude__", set())
            data = {k: v for k, v in obj.__dict__.items()
                    if k not in exclude}
            return dumper.represent_mapping(yaml_tag, data)

        def constructor(loader, node):
            data = loader.construct_mapping(node, deep=True)
            return cls(**data)

        yaml.add_representer(cls, representer)
        yaml.add_representer(StepStatus, stepstatus_representer)

        yaml.add_constructor(yaml_tag, constructor)

        return cls
    return wrapper




def read_config(path:Path):
    from ..core import StepStatus, to_Path

    import yaml
    path = to_Path(path)
    with open(path, 'r') as file:
        return yaml.load(file, Loader=yaml.FullLoader)

def write_config(config:dict, path:Path):
    from ..core import to_Path
    path = to_Path(path)
    try:
        file=open(path,"w")
        yaml.dump(config,file)
        file.close() 
    except Exception as e:
        raise RuntimeError(e)
    
    return path