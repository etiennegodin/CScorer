
from .base import BaseQuery
from shapely import wkt
from ..core import PipelineData, StepStatus
import ee, geemap
from ee.image import Image
from ee.imagecollection import ImageCollection
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
        self.date_min = data.config['time']['start']
        self.date_max = data.config['time']['end']
        
    def run(self, data:PipelineData):
        pass
        #points = self._load_occurences_points(data)
        rasters = self._load_rasters(data)
        #multi_raster = self._combine_rasters()
        #out = self._sample_occurences(multi_raster, points)
    
    def _load_rasters(self,data:PipelineData):
        datasets = data.config['gee_datasets']
        image_datasets = datasets['image']
        imageCollection_datasets = datasets['imageCollection']

        
        rasters = []

        for image in image_datasets:
            raster = Image(image) \
                .clip(self.aoi)
            rasters.append(raster)  
            
        for image_col in imageCollection_datasets:
            img_col = ImageCollection(image_col) \
                .filterBounds(self.aoi)\
                .filterDate(f"{self.date_min}", f"{self.date_max}") \
                .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 10))
            
            median = img_col.median().clip(self.aoi)
            ndvi = median.normalizedDifference(['B8', 'B4']).rename('NDVI')

            print(type(median))
        
        
        print(rasters)
        

    def _combine_rasters(self, rasters:list[Image])-> Image:
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