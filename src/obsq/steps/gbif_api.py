from ..pipeline import PipelineContext, StepStatus, ClassStep
import os
import asyncio
from pygbif import occurrences as occ
from pathlib import Path
from pprint import pprint #debug


class GbifLoader(ClassStep):
    
    def __init__(self, name:str, **kwargs):
        super().__init__(name, retry_attempts = 1,**kwargs)
        self.name = name
        
    async def _execute(self, context:PipelineContext):
        #Load gbif_loader default config
        gbif_loader_conf = context.config['gbif_loader']
        #Build predicate from config
        self.predicate = self._predicate_builder(gbif_loader_conf)
        
        #Add additonnal predicates
        if self._validate_custom_predicates(context):
            custom_predicates_dict = context.get_step_output("create_custom_predicates")
            custom_predicates = custom_predicates_dict[self.name.split(sep='.')[-1]]
            for key, value in custom_predicates.items():
                self.predicate.add_field(key = key, value = value)
                    
        # Set dict for self outputs
        if not self._validate_has_download_key(context):
            download_key = await self._submit_request()
            self.logger.info(f"- {self.name} Gbif request made. Key : {download_key}")
            context.set_intermediate_step_result(self.name, {'download_key' : download_key} )
            
        if self._validate_has_download_key(context):
            download_key = context.get_step_output(self.name)['download_key']
            ready_key = await self._poll_gbif_until_ready(download_key)
        gbif_dir = Path(context.config['paths']['gbif_folder'])
        dest_dir = gbif_dir / download_key
        file_list = await self._download_and_unpack(ready_key, dest_dir= dest_dir)
        context.set_intermediate_step_result(self.name, {"files": file_list} )

        for f in file_list:
            if "verbatim.txt" in f:
                old_path = Path(f)
                new_path = Path(f"{old_path.parent}/{download_key}.csv")  
                os.rename(old_path,new_path)
                return {"output":str(new_path)}
            

    def _validate_custom_predicates(self,context):
        return context.get_step_output("create_custom_predicates") is not None

    # Validation functions (optional)
    def _validate_has_download_key(self,context: PipelineContext) -> bool:
        step_output = context.get_step_output(self.name)
        if step_output is not None:
            return "download_key" in step_output.keys()
        return False
    
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
            self.status = StepStatus.PENDING
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
        file_list = []
        files = os.listdir(dest_dir)
        for f in files:
            file_list.append(f"{dest_dir}/{f}")
            
        os.remove(zip_path)
        return file_list
    
    
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
