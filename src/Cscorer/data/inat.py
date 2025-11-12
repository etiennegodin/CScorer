from .base import BaseQuery
from shapely import wkt
from ..core import PipelineData, StepStatus


class iNatQuery(BaseQuery):

    def __init__(self, name:str):
        super().__init__()
        self.name = name

    async def run(self, data:PipelineData):
        import webbrowser

        
        
        query = await self._build_query()
        #build query
        query = "has%5B%5D=photos&quality_grade=research&identifications=any&captive=false&swlat=45.014526+&swlng=-74.519611&nelat=46.821866+&nelng=-70.203212&not_in_place=187355&taxon_id=211194&d1=2021-01-01&d2=2025-11-01"
        url = f"https://www.inaturalist.org/observations/export?{query}"
        
        webbrowser.open(url)


        
        
        
    
    
        


    async def _build_query(self):
        pass
    


    def _get_bounds(config):
        geo = config['gbif']['GEOMETRY']
        geom = wkt.loads(geo)
        minx, miny, maxx, maxy = geom.bounds

        # South-West and North-East
        lat_so, lon_so = miny, minx
        lat_ne, lon_ne = maxy, maxx

        print(lat_so, lon_so )
        print(lat_ne, lon_ne)
        
    
    
    

async def _get_occurenceIDs(con):
    query = """
            SELECT occurrenceID,
            FROM gbif_raw.citizen
            WHERE institutionCode = 'iNaturalist';
    """
    
    occurenceURLs = con.execute(query).df()['occurrenceID']
    #occurenceIDs = occurenceURLs.apply(lambda x: x.split(sep='/')[-1]).to_list()
    
    return occurenceURLs.to_list()
