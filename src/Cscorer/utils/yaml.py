
import yaml

CLASS_REGISTRY = {}
def yaml_serializable(tag=None):
    def wrapper(cls):
        # FIX: ensure we wrap the final class, not the original
        final_cls = cls
        
        yaml_tag = tag or f"!{final_cls.__name__}"

        def representer(dumper, obj):
            return dumper.represent_mapping(
                yaml_tag,
                obj.__dict__
            )

        def constructor(loader, node):
            data = loader.construct_mapping(node, deep=True)
            return final_cls(**data)

        yaml.add_representer(final_cls, representer)
        yaml.add_constructor(yaml_tag, constructor)

        return final_cls
    return wrapper
