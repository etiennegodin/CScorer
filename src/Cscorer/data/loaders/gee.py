
from .base import BaseLoader
from ...pipeline import Pipeline, PipelineStep, StepStatus
from ...utils.duckdb import export_to_shp
from ...utils.core import to_Path
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
from pprint import pprint

class GeeLoader(BaseLoader):

    def __init__(self, pipe:Pipeline, name:str, points:str):
        super().__init__()
        # Init gee 
        self._init_gee()
        #Create geometry area of interest 
        self.aoi = ee.Geometry.Polygon(self._get_coords(pipe))
        self.points = ee.FeatureCollection(points)
        self.point_count = self.points.size().getInfo()
        #Dates
        self.date_min = pipe.config['time']['start']
        self.date_max = pipe.config['time']['end']
        
        # Create step_name from name of point dataset
        points = to_Path(points)  
        self.name = f"gee_sampling_{name}"

        #Create output dir:
        self.output_dir = self._create_dir(pipe)
    
    async def run(self, pipe:Pipeline, step:PipelineStep):
        chunk_size = pipe.config['gee']['chunk_size']
        logger = pipe.logger
        con = pipe.con
        table_name = str(self.name.split(sep='_', maxsplit=2 )[-1])
        self.df = pd.DataFrame()
        #Main
        if step.status == StepStatus.init:
            logger.info(f'Launching sampling procees for {self.name}')
            step.storage['db'] = f"gee.{table_name}"
            #Set chunks
            num_chunks = (self.point_count + chunk_size - 1) // chunk_size
            # Convert to list for indexing
            points_list = self.points.toList(self.point_count)
            #Rasters 
            rasters = await self._load_rasters(pipe)
            multi_raster = await self._combine_rasters(rasters, logger)
            
            for i in range(num_chunks):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, self.point_count)
                # Create chunk
                chunk = ee.FeatureCollection(points_list.slice(start_idx, end_idx))
                samples = await self._sample_occurences(chunk, multi_raster,logger)
                await self._store_sample_to_df(samples)
                print(f"Saved samples for chunk {i+1}/{num_chunks} (points {start_idx}-{end_idx})")
            
            #Save final samples to db
            table = await self._save_to_db(con, table_name, logger)
            logger.info(f"Finished getting gee data for {self.name}")
            step.status = StepStatus.completed
            pipe.update()
            
    def _chunk_samples(self, fc:FeatureCollection, logger, size:int = 2500):
        chunks = []
        for i in range(0, self.point_count, size):
            chunk = fc.toList(size, i)
            chunks.append(ee.FeatureCollection(chunk))
        logger.info(f"Created {len(chunk)} chunks for {self.name}")
        return chunks
            
    def _get_gee_tasks(self):
        tasks = ee.data.getTaskList()
        pprint(tasks)
        
    async def _wait_download(self, path, logger, delay = 10, retries = 10):
        data_downloaded = False
        retry = 0 
        while not data_downloaded:
            if retry > retries:
                break
            if path.exists():
                data_downloaded = True
                return True
            retry += 1
            logger.warning(f"{path.stem}{path.suffix} not found on retry {retry}/{retries}")
            await asyncio.sleep(delay)
            
    async def _save_to_db(self, con, table_name, logger):
        #Savng to db
        df = self.df
        con.execute(f"CREATE OR REPLACE TABLE gee.{table_name} AS SELECT * FROM df")
            
        logger.info(f"Successfully saved {table_name} samples to disk")
        return table_name
        
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
        
    def _create_dir(self, pipe):
        sampled_dir = Path(pipe.config['folders']['gee_folder']) / "sampled"
        sampled_dir.mkdir(exist_ok= True)
        return sampled_dir
    
    async def _load_rasters(self,pipe:Pipeline)-> list[Image]:
        datasets = pipe.config['gee_datasets']
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

        pipe.logger.info(f"Loaded {len(rasters)} rasters to sample")
        return rasters 
    
    async def _combine_rasters(self, rasters:list[Image], logger)-> Image:    
        multi_layer_raster = ee.Image.cat(rasters)
        logger.info(f"Combined rasters")
        return multi_layer_raster        
        
    async def _sample_occurences(self, point_chunk, raster:Image, logger):
        samples = raster.sampleRegions(
        collection=point_chunk,
        scale=30,
        geometries=True   # keep original geometry in output
        )
        return samples
            
    def _get_coords(self, data)->list:
        geo = wkt.loads(data.config['gbif']['GEOMETRY'])
        return list(geo.exterior.coords)
    
    def _init_gee(self):
        load_dotenv()
        try:
            ee.Authenticate()
            ee.Initialize(project=os.getenv("GEE_PROJECT_ID"))
            return True
        except Exception as e:
            raise Exception(e)
        
    
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
    print('exit')

    if step.status == StepStatus.completed:
        pipe.logger.info(f"Step {step_name} completed")
        print('exit')
        print(step.storage['points'])
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
        data_name = table.split(sep='.')[1]
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
        uploads = [asyncio.create_task(_check_asset_upload(file, pipe, step))  for file in files]
        await asyncio.gather(*uploads)
        step.status = StepStatus.completed
        pipe.logger.info('Successfully upload all files to gee')
    
    return step.storage['points']
    
    
    