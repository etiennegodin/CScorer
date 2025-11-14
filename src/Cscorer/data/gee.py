
from .base import BaseQuery
from shapely import wkt
from ..core import PipelineData, StepStatus
from ..utils.duckdb import export_to_shp
import ee, geemap
from ee.image import Image
from ee.imagecollection import ImageCollection
import os 
from dotenv import load_dotenv 
from pathlib import Path
import asyncio

class GeeQuery(BaseQuery):

    def __init__(self, data:PipelineData, name:str):
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
        self.name = name
        data.update_step_status(name, status= StepStatus.init)

        
    #Snippets
    #var dataset = ee.Image('CGIAR/SRTM90_V4');
    #var elevation = dataset.select('elevation');
    #var slope = ee.Terrain.slope(elevation);
    
    
    def run(self, data:PipelineData):
        pass
        points_path = self._upload_points(data)
        #points = self._load_occurences_points(data)
        rasters = self._load_rasters(data)
        #multi_raster = self._combine_rasters()
        #out = self._sample_occurences(multi_raster, points)
    
    def _load_rasters(self,data:PipelineData):
        datasets = data.config['gee_datasets']
        image_datasets = datasets['image']
        imageCollection_datasets = datasets['imageCollection']
        rasters = []

        for img_dataset in image_datasets:
            raster = Image(img_dataset['name']) \
                .clip(self.aoi)
            rasters.append(raster)  
            
        for image_col_dataset in imageCollection_datasets:
            img_col = ImageCollection(image_col_dataset['name']) \
                .filterBounds(self.aoi)\
                .filterDate(f"{self.date_min}", f"{self.date_max}") \
                .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 10))
            
            median = img_col.median().clip(self.aoi)        
            rasters.append(median)        
    
    def _combine_rasters(self, rasters:list[Image])-> Image:    
        multi_layer_raster = ee.Image.cat(rasters)
        return multi_layer_raster

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
        
        
async def _upload_file_to_gee(data, step_name, file, table):
    try:
        geemap.upload_to_ee(
        filename=file,
        asset_path=f"projects/{os.getenv('GEE_PROJECT')}/assets/{file.stem}",
        overwrite=True)
        return True 
    except Exception as e:
        data.logger.error(f"Error uploading occurences to gee : \n{e}")
        data.update_step_status(step_name, StepStatus.failed)
    
async def upload_points(data:PipelineData ):
    #Upload points to gee 
    step_name = 'upload_occurences_point_to_gee'
    data.init_new_step(step_name=step_name)

    tables = []
    files = []
    steps = [step for step in data.storage.keys() if step.startswith('gbif')]
    for step in steps:
        table = data.storage[step]['db']
        tables.append(table)
        files.append(Path(f"{table.split(sep='.')[1]}_occurences.shp"))
    
    #Export table points to disk
    exports = [asyncio.create_task(export_to_shp(con = data.con, file_path = file, table_name = table, table_fields= 'gbifID')) for file, table in zip(files,tables)]
    await asyncio.gather(*exports)
    data.update_step_status(step_name, StepStatus.local)

    return 
    #Upload these points to gee 
    uploads = [asyncio.create_task(_upload_file_to_gee(data, step_name, file, table))  for file, table in zip(files,tables)]
    await asyncio.gather(*exports)

    data.update_step_status(step_name, StepStatus.completed)
    
    

            
