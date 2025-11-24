from .gee import GeeLoader
from .gbif import GbifLoader
from .inat import iNatOccLoader, iNatObsLoader

QUERY_CLASSES = {
    "gbif": GbifLoader,
    "inatOcc" : iNatOccLoader,
    "inatObs" : iNatObsLoader,
    "gee": GeeLoader
}

def create_query(kind: str, *args, **kwargs):
    if kind not in QUERY_CLASSES:
        raise NotImplementedError(f"Unknown query type: {kind}")
    return QUERY_CLASSES[kind](*args,**kwargs)