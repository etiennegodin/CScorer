from .base import BaseQuery

class iNatQuery(BaseQuery):

    def run(self, config:dict):
        pass
        

async def get_occurenceIDs(con):
    query = """
            SELECT occurrenceID,
            FROM gbif_raw.citizen
            WHERE institutionCode = iNaturalist;
    """
    
    occurenceURLs = con.execute(query).df()['occurrenceID']
    occurenceIDs = occurenceURLs.apply(lambda x: x.split(sep='/')[-1]).to_list()
    return occurenceIDs

token = "eyJhbGciOiJIUzUxMiJ9.eyJ1c2VyX2lkIjo0NDE1NDY3LCJleHAiOjE3NjI5ODI1ODl9.bEPjem-1nSLeViU0lDyYBJ9UFnR2w2VXqvfVikhp8OADBmoj4Hm8fZpAkFJ--BcLezW5o0NL3wdTfqU5pOxSqA"

import requests
import time

async def inat_main(data):
    con = data.con 
    
    occurenceIDs = await get_occurenceIDs(con)
    occurenceIDs = occurenceIDs[:5]
    # You'll need to be authenticated
    headers = {
        'Authorization': f'Bearer {token}',  # Get from iNaturalist account
        'Content-Type': 'application/json'
    }

    # Create export with your observation IDs
    data = {
        'id': ','.join(map(str, occurenceIDs)),  # comma-separated list
        # Or use other filters instead of specific IDs:
        # 'taxon_id': 12345,
        # 'place_id': 67890,
        # 'user_id': 'username',
        # 'd1': '2024-01-01',  # date range
        # 'd2': '2024-12-31'
    }

    response = requests.post(
        'https://api.inaturalist.org/v1/observation_exports',
        headers=headers,
        json=data
    )

    export_id = response.json()['id']
    print(f"Export created: {export_id}")