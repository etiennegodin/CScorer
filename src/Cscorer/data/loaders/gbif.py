import os
import asyncio
import logging
from pathlib import Path
from pygbif import occurrences as occ
from ...core import Pipeline, PipelineStep, StepStatus
from .inat import BaseLoader

class GbifLoader(BaseLoader):
    def __init__(self, name:str, config:dict):
        super().__init__()
        self.name = name
        self.config = config
        self.predicate = self._predicate_builder(self.config)
        
    async def run(self, pipe:Pipeline, step:PipelineStep):
        logger = pipe.logger
        step_name = self.name
        # Set dict for step outputs 
        
        if "key" not in step.storage.keys():
            if step.status == StepStatus.init:
                download_key = await self._submit_request(pipe)
                step.storage['key'] = download_key
                logger.info(f"- {step_name} Gbif request made. Key : {download_key}")
                step.status = StepStatus.requested
        else:
            download_key = step.storage['key']
            step.status = StepStatus.requested

        if step.status[f'{step_name}'] == StepStatus.requested:
            ready_key = await self._poll_gbif_until_ready(download_key, logger= pipe.logger)
            step.status = StepStatus.ready

        if step.status[f'{step_name}'] == StepStatus.ready:
            gbif_raw_data = await self._download_and_unpack(ready_key, dest_dir= pipe.config['folders']['gbif_folder'], logger= pipe.logger)
            step.storage['raw_data'] = gbif_raw_data
            step.status = StepStatus.local
            pipe.update()

        return gbif_raw_data
    
    async def _submit_request(self, pipe:Pipeline):  
        logger = pipe.logger
        from dotenv import load_dotenv
        env_path = Path(__file__).parent.parent.parent.parent / ".env"
        load_dotenv(env_path)
        try:
            query = self.predicate.to_dict() # build 
            #threaded as returns tuples 
            response = await asyncio.to_thread(lambda: occ.download(query,
                    format="SIMPLE_CSV",
                    user= str(os.getenv("GBIF_USERNAME")),
                    pwd=str(os.getenv("GBIF_PASSWORD")),
                    email=str(os.getenv("GBIF_EMAIL"))))
            
        except Exception as e:
            logger.error(f"Error running gbif request: {e}")
            raise RuntimeError(e)
        if response:   
            return response[0]
    
    async def _poll_gbif_until_ready(self, download_key:str, logger = logging.Logger, poll_interval: int = 30, timeout_seconds: int = 60*60, ):
        
        """
        Poll GBIF for the download status. Uses occ.download_meta or occ.download_get to inspect.
        We return once status == 'SUCCEEDED' and the download URL/identifier is available.
        """
        import time
        import asyncio
        start = time.time()

        while True:
            meta =  await asyncio.to_thread(lambda: occ.download_meta(key=download_key))
            logger.debug(meta)
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

    async def _download_and_unpack(self, download_key:str, dest_dir: str, logger = logging.Logger):
        """
        Fetch the GBIF ZIP (SQL_TSV_ZIP or SIMPLE_CSV) and unpack to dest_dir.
        pygbif.occurrence.download_get can write the file locally.
        """
 
        os.makedirs(dest_dir, exist_ok=True)
        logger.info(f"Downloading GBIF data for {download_key} to {dest_dir}...")
        # This returns file path or bytes depending on client; example below writes to path
        response = await asyncio.to_thread(lambda: occ.download_get(download_key, path=dest_dir))
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
        os.remove(zip_path)
        return output
    
    
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
    
    
class GbifPredicate():
    
    def __init__(self, type = 'and'):
        if type not in ('and', 'or'):
            raise ValueError("Mode must be 'and' or 'or'")
        self.type = type
        self.predicates = []
        
    def add_field(self, key :str, value:str, type :str = 'equals'):
        if isinstance(value, list):
            # Requires type to be "in" for list 
            expr = {"type": 'in', "key": key, "values":value}
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
            self.predicates.append({'type': 'equals', 'key' : "HAS_COORDINATE", "value" :  'true'}) # Add coords flag 
            self.predicates.append({'type': 'equals', 'key' : "OCCURRENCE_STATUS", "value" :  'present'}) # Add coords flag 

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
