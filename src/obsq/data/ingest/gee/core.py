from ....pipeline import ClassStep, PipelineContext, step
from ....utils.duckdb import _open_connection
import pandas as pd
from shapely import wkt
import ee
from ee.image import Image
from ee.imagecollection import ImageCollection
from ee.featurecollection import FeatureCollection
import os 
from dotenv import load_dotenv 
from pathlib import Path
import asyncio
import json
from pprint import pprint
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union, Callable
from shapely import to_geojson, wkt


def string_to_coords(geo_string)->list:
    geo = wkt.loads(geo_string)
    return list(geo.exterior.coords)

def wkt_string_to_geojson(wkt_string:str)->dict:

    geometry = wkt.loads(wkt_string)
    geo_json_string = to_geojson(geometry)
    geojson_dict = json.loads(geo_json_string)
    return geojson_dict


@dataclass
class GeeContext:
    aoi: ee.Geometry.Polygon
    points: ee.FeatureCollection
    date_min: str
    date_max:str
    
def init_gee():
    load_dotenv()
    try:
        ee.Authenticate()
        ee.Initialize(project=os.getenv("GEE_PROJECT_ID"))
        return True
    except Exception as e:
        raise Exception(e)
class GeeRandomPoints(ClassStep):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

class GeePointSampler(ClassStep):
    def __init__(self, name, gee_context:GeeContext, scale:int = 30 ,**kwargs):
        self.gee_context = gee_context
        self.chunk_size = 5000
        self.table_name = f"raw.gee_{name}"
        self.point_count = self.gee_context.points.size().getInfo()
        self.scale = scale
        self.df = pd.DataFrame()
        super().__init__(name, **kwargs)

    async def _execute(self, context:PipelineContext):
        self.logger.info(f'Launching sampling process for {self.name}')

        #Set chunks
        num_chunks = (self.point_count + self.chunk_size - 1) // self.chunk_size
        # Convert to list for indexing
        points_list = self.gee_context.points.toList(self.point_count)
        #Rasters 
        rasters = await self._load_rasters(context)
        multi_raster = await self._combine_rasters(rasters)

        for i in range(num_chunks):
            start_idx = i * self.chunk_size
            end_idx = min((i + 1) * self.chunk_size, self.point_count)
            # Create chunk
            chunk = ee.FeatureCollection(points_list.slice(start_idx, end_idx))
            samples = await self._sample_occurences(chunk, multi_raster)
            await self._store_sample_to_df(samples)
            print(f"Saved samples for chunk {i+1}/{num_chunks} (points {start_idx}-{end_idx})")
        #Save final samples to db
        table = await self._save_to_db(context, self.table_name)
        self.logger.info(f"Finished getting gee data for {self.name}")


        
    async def _load_rasters(self,context:PipelineContext)-> list[Image]:
        datasets = context.config['gee_datasets']
        image_datasets = datasets['image']
        imageCollection_datasets = datasets['imageCollection']
        rasters = []

        for img_dataset in image_datasets:
            raster = Image(img_dataset['name']) \
                .clip(self.gee_context.aoi)
            rasters.append(raster)  
            
        for image_col_dataset in imageCollection_datasets:
            img_col = ImageCollection(image_col_dataset['name']) \
                .filterBounds(self.gee_context.aoi)\
                .filterDate(f"{self.gee_context.date_min}", f"{self.gee_context.date_max}") \
                .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 10))
            
            median = img_col.median().clip(self.gee_context.aoi)        
            rasters.append(median)        

        self.logger.info(f"Loaded {len(rasters)} rasters to sample")
        return rasters 
    

    async def _combine_rasters(self, rasters:list[Image])-> Image:    
        multi_layer_raster = ee.Image.cat(rasters)
        self.logger.info(f"Combined rasters")
        return multi_layer_raster        
        
    async def _sample_occurences(self, point_chunk, raster:Image):
        samples = raster.sampleRegions(
        collection=point_chunk,
        scale=self.scale,
        geometries=True   # keep original geometry in output
        )
        return samples
    
    async def _store_sample_to_df(self,samples):
        samples_data = []
        for feature in samples.getInfo()['features']:
            data = feature['properties']
            data['gee_id'] = feature["id"]
            data['coordinates'] = feature['geometry']['coordinates']
            samples_data.append(data)
        #Samples data to df
        df_temp = pd.DataFrame(samples_data)
        self.df = pd.concat([self.df, df_temp], ignore_index=True)

    async def _save_to_db(self, context, table_name):
        #Savng to db
        df = self.df
        con = _open_connection(context.get_step_output("db_connection"))
        context.con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        self.logger.info(f"Successfully saved {table_name} samples to disk")
        return table_name
        
"""
async def _check_asset_upload(file:Path, pipe:Pipeline, step:PipelineStep):
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
            pipe.logger.info(f" {file.stem}{file.suffix} successfully uploaded to gee")
            step.storage['points'][file.stem] = asset_id
            pipe.update()

            confirmed = True
        else:
            pipe.logger.info(f"Please manually upload {file.stem}{file.suffix} to gee project - Sleeping for {delay}s")
            pipe.logger.warning(stdout.decode())

            await asyncio.sleep(delay)
            
async def upload_points(pipe:Pipeline, step :PipelineStep):

    step_name = 'upload_occurences_point_to_gee'
    output_folder = pipe.config['folders']['gee_folder']
    tables = []
    files = []
    
    if step.status == StepStatus.completed:
        pipe.logger.info(f"Step {step_name} completed")
        return step.storage['points'] 
       
    #Init step if not done 
    step.storage['points'] = {}

    # Create files and table lookups    
    steps = [s for s in step._parent.steps.values() if "gbif" in s.name]
        
    # Create files and table lookups    
    for s in steps:
        table = s.storage['db']
        tables.append(table)
        #Make folder 
        data_name = table.split(sep='_')[1]
        sub_folder = Path(f"{output_folder}/{data_name}")
        sub_folder.mkdir(exist_ok= True)
        files.append(sub_folder / f"{data_name}_occurences.shp")
        
    #Export to shp
    if step.status == StepStatus.init:
        #Export table points to disk
        exports = [asyncio.create_task(export_to_shp(con = pipe.con, file_path = file, table_name = table, table_fields= 'gbifID', logger = pipe.logger)) for file, table in zip(files,tables)]
        await asyncio.gather(*exports)
        step.status = StepStatus.local

    if step.status == StepStatus.local:
        #Upload these points to gee 
        uploads = [asyncio.create_task(_check_asset_upload(file, pipe, step)) for file in files]
        await asyncio.gather(*uploads)
        step.status = StepStatus.completed
        pipe.logger.info('Successfully upload all files to gee')
    
    return step.storage['points']
    
    """
    