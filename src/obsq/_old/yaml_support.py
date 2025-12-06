
import yaml
from pathlib import Path
import enum

CLASS_REGISTRY = {}

def yaml_serializable(tag=None):
    from yaml.nodes import MappingNode

    def wrapper(cls):

        yaml_tag = tag or f"!{cls.__name__}"
        CLASS_REGISTRY[yaml_tag] = cls

        def representer(dumper, obj):
            exclude = getattr(obj, "__yaml_exclude__", set())
            data = {}
            for k, v in obj.__dict__.items():
                if k in exclude:
                    continue
                if isinstance(v, enum.Enum):
                    data[k] = v
                    continue
                data[k] = v
            return dumper.represent_mapping(yaml_tag, data)

        def constructor(loader, node):
            if not isinstance(node, MappingNode):
                raise TypeError(f"{yaml_tag} must be applied to a mapping node")

            data = loader.construct_mapping(node, deep=True)
            return cls(**data)

        yaml.add_representer(cls, representer)
        yaml.add_constructor(yaml_tag, constructor)

        return cls
    return wrapper

def read_config(path:Path):
    from ..utils.core import to_Path

    import yaml
    path = to_Path(path)
    with open(path, 'r') as file:
        return yaml.load(file, Loader=yaml.FullLoader)

