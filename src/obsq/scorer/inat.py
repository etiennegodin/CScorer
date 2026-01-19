import aiohttp
from ..pipeline import *
from typing import Union, Literal

request_types = Literal['id', 'user_id']

class inatObservationApi:
    def __init__(self, type:request_types, ids:Union[str,list],**kwargs):

        if isinstance(ids,list):
            self.ids = self._list_to_string(ids)
        elif isinstance(ids, int):
            self.ids = str(ids)
        elif isinstance(ids, str):
            self.ids = ids

        fields = ['quality_grade',
                  'observed_on_string'
                  'description',
                  'positional_accuracy',
                  {'identifications': ['user']},
                  {'annotations' : ['user_id','controlled_attribute_id','controlled_value_id' ]},
                    'observed_on_details',
                    'num_identification_agreements',
                    'identification_disagreements_count',
                    {'taxon':['id','rank']},
                    'uri',
                    'location',
                    'user',
                    'identifications',
                    'photos',
                    'community_taxon',
                    'outlinks',
        ]

        self.params = {str(type) : self.ids, 
                       'taxon_id' : '47126', #plantae
                       'quality_grade' : 'research', #research grade only
                       'rank' : 'species', #species only for now
                       'fields': fields_to_string_v2(fields)}
        
        print(self.params)

        self.base_url = "https://api.inaturalist.org/v2/observations/"


    async def request(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url, params=self.params, timeout=10) as r:
                                print(r.url)
                                r.raise_for_status()
                                data = await r.json()    
                                if data and "results" in data: 
                                    return data['results']

    
    def _list_to_string(self, ids:list)->str:
        output = ""
        max = len(ids)
        for e, i in enumerate(ids):
            if e + 1 == max :
                output += i
                break
            output += f"{i},"

        return output
    


class inatUserApi():
    def __init__(self):

        self.base_url = "https://api.inaturalist.org/v2/observations/"

        pass

    def request(self):
         pass

# Convert to the special syntax
def fields_to_string_v2(fields_list:list, level=0):
    parts = []
    for field in fields_list:
        if isinstance(field, str):
            parts.append(field)
        elif isinstance(field, dict):
            for field_key, field_items in field.items():
                for i in field_items:
                    parts.append(f'{field_key}.{i}')
    return ','.join(parts)