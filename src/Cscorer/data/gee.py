
from .base import BaseQuery
from ..core import PipelineData, StepStatus, to_Path
from ..utils.duckdb import export_to_shp
from ..utils.core import _ask_yes_no

from shapely import wkt
import ee, geemap
from ee.image import Image
from ee.imagecollection import ImageCollection
import os 
from dotenv import load_dotenv 
from pathlib import Path
import asyncio
import subprocess 
import time


def init_gee():
    load_dotenv()
    try:
        ee.Authenticate()
        ee.Initialize(project=os.getenv("GEE_PROJECT_ID"))

        return True
    except Exception as e:
        raise Exception(e)


class GeeQuery(BaseQuery):

    def __init__(self, data:PipelineData, name:str):
        super().__init__()
        # Init gee 
        init_gee()
        
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
        
        
async def _upload_file_to_gee(data, file:Path):
    if not isinstance(file, Path):
        file = to_Path(file)
        
    folder = str(file.parent) + "/"
    command = ["earthengine",
               f"--project={str(os.getenv('GEE_PROJECT_ID'))}",
               "upload","table",
               f"--asset_id=projects/{os.getenv('GEE_PROJECT_ID')}/assets/{file.stem}",
               folder # positional arg for shp folder
               ]
    print(command)
    try:
        result = subprocess.run(command, check= True)
        return result 
    except Exception as e:
        data.logger.error(f"Error uploading occurences to gee : \n{e}")
        #data.update_step_status(step_name, StepStatus.failed)
        raise Exception(e)
    
async def _check_asset_upload(file:Path, data:PipelineData, step_name):
    delay = 15
    load_dotenv()
    if not isinstance(file, Path):
        file = to_Path(file)
        
    folder = str(file.parent) + "/"
    confirmed = False
    asset_id = f"projects/{os.getenv('GEE_PROJECT_NAME').lower()}/assets/{file.stem}"
    command = ["earthengine",
               f"--project={str(os.getenv('GEE_PROJECT_ID'))}",
               "asset","info",
               asset_id,
               ]
    #while not confirmed:
    while not confirmed:
        proc = await asyncio.create_subprocess_exec(*command,
                                                    stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()

        if proc.returncode == 0:
            data.logger.info(f" {file.stem}{file.suffix} successfully uploaded to gee")
            data.storage[step_name]['points'].append(asset_id)

            confirmed = True
        else:
            data.logger.info(f"Please manually upload {file.stem}{file.suffix} to gee project - Sleeping for {delay}s")
            data.logger.warning(stdout.decode())

            await asyncio.sleep(delay)
            
async def upload_points(data:PipelineData ):

    step_name = 'upload_occurences_point_to_gee'
    output_folder = data.config['folders']['gee_folder']
    user_upload = False
    tables = []
    files = []

    if data.step_status[step_name] == StepStatus.completed:
        data.logger.info(f"{step_name} completed")
        return
    
    #Init step if not done 
    data.init_new_step(step_name=step_name)
    data.storage[step_name]['points'] = []

    # Create files and table lookups    
    steps = [step for step in data.storage.keys() if step.startswith('gbif')]

    # Create files and table lookups    
    for step in steps:
        table = data.storage[step]['db']
        tables.append(table)
        #Make folder 
        data_name = table.split(sep='.')[1]
        sub_folder = Path(f"{output_folder}/{data_name}")
        sub_folder.mkdir(exist_ok= True)
        files.append(sub_folder / f"{data_name}_occurences.shp")
        
    #Export to shp
    if data.step_status[step_name] == StepStatus.init:
        #Export table points to disk
        exports = [asyncio.create_task(export_to_shp(con = data.con, file_path = file, table_name = table, table_fields= 'gbifID', logger = data.logger)) for file, table in zip(files,tables)]
        await asyncio.gather(*exports)
        data.update_step_status(step_name, StepStatus.local)

    if data.step_status[step_name] == StepStatus.local:
        #Upload these points to gee 
        uploads = [asyncio.create_task(_check_asset_upload(file, data, step_name))  for file in files]
        await asyncio.gather(*uploads)
        data.update_step_status(step_name, StepStatus.completed)
        data.logger.info('Successfully upload all files to gee')
        
    