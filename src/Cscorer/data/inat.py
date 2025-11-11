from .base import BaseQuery

class iNatQuery(BaseQuery):

    def run(self, config:dict):
        pass
        

token = "eyJhbGciOiJIUzUxMiJ9.eyJ1c2VyX2lkIjo0NDE1NDY3LCJleHAiOjE3NjI5ODI1ODl9.bEPjem-1nSLeViU0lDyYBJ9UFnR2w2VXqvfVikhp8OADBmoj4Hm8fZpAkFJ--BcLezW5o0NL3wdTfqU5pOxSqA"

import requests
import time

async def inat_main(data):
    con = data.con 
    
    # You'll need to be authenticated
    headers = {
        'Authorization': f'Bearer {token}',  # Get from iNaturalist account
        'Content-Type': 'application/json'
    }

    # Create export with your observation IDs
    data = {
        'id': ','.join(map(str, your_observation_ids)),  # comma-separated list
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