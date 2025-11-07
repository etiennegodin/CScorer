from Cscorer.data.query import get_data
from Cscorer.core import read_config
from pathlib import Path

path = Path(__file__).parent / "gbif_query.yaml"
config = read_config(path)
query = get_data('gbif', config)
print(query.predicate)
query.run()
