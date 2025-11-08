from .env import GeeQuery
from .gbif import GbifQuery
from .inat import iNatQuery

QUERY_CLASSES = {
    "gbif": GbifQuery,
    "inat" : iNatQuery,
    "env": GeeQuery
}

def create_query(kind: str, *args, **kwargs):
    if kind not in QUERY_CLASSES:
        raise NotImplementedError(f"Unknown query type: {kind}")
    return QUERY_CLASSES[kind](*args,**kwargs)