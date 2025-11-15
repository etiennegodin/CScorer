
import yaml

CLASS_REGISTRY = {}

def yaml_serializable(tag=None):
    """
    Decorator to register a custom class for YAML dumping/loading.
    """
    def wrapper(cls):
        yaml_tag = tag or f"!{cls.__name__}"
        CLASS_REGISTRY[yaml_tag] = cls

        # --- Representer (Python → YAML) ---
        def representer(dumper, obj):
            return dumper.represent_mapping(
                yaml_tag,
                obj.__dict__
            )

        # --- Constructor (YAML → Python) ---
        def constructor(loader, node):
            data = loader.construct_mapping(node, deep=True)
            return cls(**data)

        yaml.add_representer(cls, representer)
        yaml.add_constructor(yaml_tag, constructor)

        return cls
    return wrapper


# Custom YAML handlers
def stepstatus_representer(dumper, data):
    return dumper.represent_scalar("!StepStatus", data.value)

def stepstatus_constructor(loader, node):
    value = loader.construct_scalar(node)
    return StepStatus(value)