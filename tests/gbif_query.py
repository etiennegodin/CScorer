from Cscorer.data.query import get_data


config = { "BOUNDS":[-73.591232,-73.583851,45.513204,45.517895],
"TAXON_KEY": 7707728
}

query = get_data('gbif', config)

print(query.predicate)