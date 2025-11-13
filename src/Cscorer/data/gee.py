
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
        
    def run(self, data:PipelineData):
        pass
        points = self._load_occurences_points(data)
        rasters = self._load_rasters
        multi_raster = self._combine_rasters()
        out = self._sample_occurences(multi_raster, points)
    
    def _load_rasters(data:PipelineData):
        datasets = data.config['gee_datasets']
        rasters = []
        
        pass

    def _combine_rasters(self, rasters:list):
        pass
    
        multi_layer_raster = ee.Image.cat(rasters)
        return multi_layer_raster
        samples = image.sampleRegions(points, scale=10, geometries=True)

    def _load_occurences_points(self,data:PipelineData):
        
        points = ee.FeatureCollection('path/to/your/points')     
        
    def _sample_occurences(self, raster, points):
    
        samples = raster.sampleRegions(
        collection=points,
        scale=10,
        geometries=True   # keep original geometry in output
        )
    
        task = ee.batch.Export.table.toDrive(
        collection=samples,
        description='raster_samples',
        fileFormat='CSV'
    )
        task.start()