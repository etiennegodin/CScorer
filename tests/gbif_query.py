from Cscorer.data.factory import create_query
from Cscorer.core import read_config
from pathlib import Path

path = Path(__file__).parent / "gbif_query.yaml"
config = read_config(path)
query = create_query('gbif', config)
print(query.predicate)
query.run()
