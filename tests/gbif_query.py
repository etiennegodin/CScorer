from pygbif import occurrences as occ
from pprint import pprint

response = occ.search(taxonKey= 7707728, datasetKey="2fd02649-fc08-4957-9ac5-2830e072c097", country= "CA", limit= 2)

pprint(response['results'])

