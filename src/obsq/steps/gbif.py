from ..pipeline import PipelineContext, StepStatus, ClassStep
import os
import asyncio
import logging
from pygbif import occurrences as occ

class GbifLoader(ClassStep):
    
    def __init__(self, name:str, predicates:dict, **kwargs):
        super().__init__(name, **kwargs)
        self.name = name
        if not isinstance(predicates, (dict, None)):
                raise ValueError("Pedicates must be dict")
        self.custom_predicates = predicates
        
    async def _execute(self, context:PipelineContext):
        #Load gbif_loader default config
        gbif_loader_conf = context.config['gbif_loader']
        #Build predicate from config
        self.predicate = self._predicate_builder(gbif_loader_conf)
        #Add additonnal predicates
        for key, value in self.custom_predicates.items():
            self.predicate.add_field(key = key, value = value)
            
        step_name = self.name
        # Set dict for self outputs 
        if not self._validate_has_no_download_key(context):
            print('here')
            download_key = await self._submit_request()
            self.logger.info(f"- {step_name} Gbif request made. Key : {download_key}")
            context.set(f'{self.name}_download_key', download_key )
        if self._validate_has_no_download_key(context):
            download_key = context.get('{self.name}_download_key')
            print('there')
        
            ready_key = await self._poll_gbif_until_ready(self,download_key)
            self.status = StepStatus.ready
        return {'x':'y'}
        if self.status == StepStatus.ready:
            gbif_raw_data = await self._download_and_unpack(self,ready_key, dest_dir= context.config['paths']['gbif_folder'])
            self.storage['raw_data'] = gbif_raw_data
            self.status = StepStatus.LOCAL
            for f in gbif_raw_data:
                if "verbatim.txt" in f:
                    output = f
                    break

        return output
    # Validation functions (optional)
    def _validate_has_no_download_key(self,context: PipelineContext) -> bool:
        return context.get('{self.name}_download_key') is not None
    
    async def _submit_request(self):  
        from dotenv import load_dotenv
        load_dotenv()
        
        try:
            query = self.predicate.to_dict() # build 
            #threaded as returns tuples 
            response = await asyncio.to_thread(lambda: occ.download(query,
                    format="DWCA",
                    user= str(os.getenv("GBIF_USERNAME")),
                    pwd=str(os.getenv("GBIF_PASSWORD")),
                    email=str(os.getenv("GBIF_EMAIL"))))
            
        except Exception as e:
            self.logger.error(f"Error running gbif request: {e}")
            self.status = StepStatus.FAILED
            raise RuntimeError(e)
        if response:   
            return response[0]
    
    async def _poll_gbif_until_ready(self, download_key:str, poll_interval: int = 30, timeout_seconds: int = 60*60, ):
        
        """
        Poll GBIF for the download status. Uses occ.download_meta or occ.download_get to inspect.
        We return once status == 'SUCCEEDED' and the download URL/identifier is available.
        """
        import time
        import asyncio
        start = time.time()

        while True:
            meta = await asyncio.to_thread(lambda: occ.download_meta(key=download_key))
            self.logger.debug(meta)
            status = meta.get("status")
            self.logger.info(f"GBIF status for {download_key}: {status}")
            if status and status.upper() in ("SUCCEEDED", "COMPLETED", "FINISHED"):
                self.logger.info("GBIF download ready.")
                return download_key
            if status and status.upper() in ("KILLED", "FAILED", "ERROR"):
                self.status = StepStatus.FAILED
                raise RuntimeError(f"GBIF download FAILED: {meta}")
            if time.time() - start > timeout_seconds:
                self.status = StepStatus.FAILED
                raise TimeoutError("Timed out waiting for GBIF download.")
            
            self.status = StepStatus.pending
            time.sleep(poll_interval)

    async def _download_and_unpack(self, download_key:str, dest_dir: str, ):
        """
        Fetch the GBIF ZIP (SQL_TSV_ZIP or SIMPLE_CSV) and unpack to dest_dir.
        pygbif.occurrence.download_get can write the file LOCALly.
        """
 
        os.makedirs(dest_dir, exist_ok=True)
        self.logger.info(f"Downloading GBIF data for {download_key} to {dest_dir}...")
        # This returns file path or bytes depending on client; example below writes to path
        response = await asyncio.to_thread(lambda: occ.download_get(download_key, path=dest_dir))
        zip_path = response['path']# implementation detail from pygbif
        self.logger.info(f"Downloaded to {zip_path}")
        # Unpack (if needed) - example uses unzip via python
        import zipfile
        if zipfile.is_zipfile(zip_path):
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(dest_dir)
            self.logger.info("Unpacked ZIP.")
        
        # Save data path
        #output = f"{dest_dir}/{response['key']}.csv"
        output = []
        files = os.listdir(dest_dir)
        for f in files:
            output.append(f"{dest_dir}/{f}")
            
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
