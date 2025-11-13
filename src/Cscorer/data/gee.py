
from .base import BaseQuery
from shapely import wkt
from ..core import PipelineData, StepStatus
import ee, geemap
import os 
from dotenv import load_dotenv 




class GeeQuery(BaseQuery):

    def __init__(self, data:PipelineData):
        super().__init__()
        # Initilaise gee 
        load_dotenv()
        ee.Authenticate()
        ee.Initialize(project=os.getenv("GEE_PROJECT"))
        
        #Create geometry area of interest 
        geo = wkt.loads(data.config['gbif']['GEOMETRY'])
        coords = list(geo.exterior.coords)
        self.aoi = ee.Geometry.Polygon(coords)
        
        
    def run(self, config:dict):
        
        pass
        
   