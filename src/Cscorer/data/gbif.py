import os
from pathlib import Path
from .base import BaseQuery
from pygbif import occurrences as occ
from ..core import PipelineData, StepStatus
import logging
from enum import Enum

class GbifQuery(BaseQuery):

    def __init__(self, config:dict):
        super().__init__()
        self.config = config
        self.predicate = self._predicate_builder(self.config)

    async def run(self, data:PipelineData, step_name:str):
        logger = data.logger

        if "gbif_download_key" not in data.storage.keys():
            if self.predicate:
                download_key = await self._submit_request(data)
                data.update_step_status(step_name, StepStatus.requested)

        ready_key = await self._poll_gbif_until_ready(data)
        data.update_step_status(step_name, StepStatus.ready)

        gbif_raw_data = await self._download_and_unpack(data, data.config['folders']['data_folder'])
        data.update_step_status(step_name, StepStatus.local)
        
        return gbif_raw_data
            
        
    def _predicate_builder(self,config:dict):
        predicate = GbifPredicate()
        for key, value in config.items():
            if key == 'LIMIT':
                continue
            if key == 'GEOMETRY':
                predicate.add_geometry(value)
                continue
            if key == 'YEAR':
                predicate.add_field(key,value, type = 'greaterThan') # custom for year 
                continue
            predicate.add_field(key, value)
        return predicate
    
    async def _submit_request(self, data:PipelineData):  
        logger = data.logger
        from dotenv import load_dotenv
        env_path = Path(__file__).parent.parent.parent.parent / ".env"
        load_dotenv(env_path)
        try:
            query = self.predicate.to_dict() # build 
            response = occ.download(query,
                    format="SIMPLE_CSV",
                    user= str(os.getenv("GBIF_USERNAME")),
                    pwd=str(os.getenv("GBIF_PASSWORD")),
                    email=str(os.getenv("GBIF_EMAIL"))
            )   
            logging.info(f'Gbif query returned : {response[0]}')
        except Exception as e:
            logger.error(e)
            raise RuntimeError(e)

        if response:   
            data.set('gbif_download_key', response[0])
            
        return response[0]
    
    async def _poll_gbif_until_ready(self, data: PipelineData, poll_interval: int = 60, timeout_seconds: int = 60*60):
        
        """
        Poll GBIF for the download status. Uses occ.download_meta or occ.download_get to inspect.
        We return once status == 'SUCCEEDED' and the download URL/identifier is available.
        """
        import time
        import asyncio
        download_key = data.get("gbif_download_key")
        logger = data.logger
        start = time.time()

        while True:
            meta = occ.download_meta(key=download_key)
            data.logger.debug(meta)
            status = meta.get("status")
            logger.info(f"GBIF status for {download_key}: {status}")
            if status and status.upper() in ("SUCCEEDED", "COMPLETED", "FINISHED"):
                logger.info("GBIF download ready.")
                return download_key
            if status and status.upper() in ("KILLED", "FAILED", "ERROR"):
                raise RuntimeError(f"GBIF download failed: {meta}")
            if time.time() - start > timeout_seconds:
                raise TimeoutError("Timed out waiting for GBIF download.")
            
            time.sleep(poll_interval)

    async def _download_and_unpack(self, data:PipelineData, dest_dir: str):
        """
        Fetch the GBIF ZIP (SQL_TSV_ZIP or SIMPLE_CSV) and unpack to dest_dir.
        pygbif.occurrence.download_get can write the file locally.
        """
        logger = data.logger
        download_key = data.get("gbif_download_key")
 
        os.makedirs(dest_dir, exist_ok=True)
        logger.info(f"Downloading GBIF data for {download_key} to {dest_dir}...")
        # This returns file path or bytes depending on client; example below writes to path
        response = occ.download_get(download_key, path=dest_dir)
        zip_path = response['path']# implementation detail from pygbif
        logger.info(f"Downloaded to {zip_path}")
        # Unpack (if needed) - example uses unzip via python
        import zipfile
        if zipfile.is_zipfile(zip_path):
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(dest_dir)
            logger.info("Unpacked ZIP.")
        
        # Save data path
        output = f"{dest_dir}/{response['key']}.csv"
        data.set('gbif_raw_data', output)
        os.remove(zip_path)
        return output

class GbifPredicate():
    def __init__(self, type = 'and'):
        if type not in ('and', 'or'):
            raise ValueError("Mode must be 'and' or 'or'")
        self.type = type
        self.predicates = []
    def add_field(self, key :str, value:str, type :str = 'equals'):
        if isinstance(value, list):
            # Requires type to be "in" for list 
            expr = {"type": 'in', "key": key, "value":value}
        else:
            expr = {"type": type, "key": key, "value":value}

        self.predicates.append(expr)
        return self
    
    def add_geometry(self, value, type = 'within'):
        expr = {"type": type, "geometry":value}
        self.predicates.append(expr)
        return self
    
    def build(self):
        if not self.predicates:
            return {}
        else:
            self.predicates.append({'type': 'isNotNull', 'parameter': 'YEAR'}) # Add year flag 
            return {
                    "type" : self.type,
                    "predicates" : self.predicates
                }
                
    def to_dict(self):
        import json
        expr = json.dumps(self.build(), indent=2)
        return json.loads(expr)
            
    def __repr__(self):
        import json
        return json.dumps(self.build(), indent=2)
