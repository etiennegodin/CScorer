from .base import BaseLoader
from shapely import wkt
from ...pipeline import Pipeline, PipelineStep, StepStatus
from ...utils.core import _ask_yes_no
from ...utils.duckdb import import_csv_to_db, get_all_tables, create_schema
import asyncio, aiohttp, aiofiles
from aiolimiter import AsyncLimiter
from asyncio import Queue
from pprint import pprint
import json
from concurrent.futures import ThreadPoolExecutor
from functools import partial

class iNatObsLoader(BaseLoader):
    def __init__(self, name:str):
        super().__init__()
        self.name = name

    async def run(self, pipe:Pipeline, step: PipelineStep):
        limit = pipe.config['inat_api']['limit']
        overwrite = pipe.config['inat_api']['overwrite']
        con = pipe.con
        logger = pipe.logger
        self.limiter = AsyncLimiter(pipe.config['inat_api']['max_calls_per_minute'], 60)
        step_name = self.name
        self.queue  = Queue()
        self.table_name = "raw.inat_observers"
        if step.status == StepStatus.completed:
            logger.info(f"{step_name} already completed")
            #SKip 
            return self.table_name
         
        
        # Create table for data
        con.execute(f"CREATE TABLE IF NOT EXISTS  {self.table_name} (id INTEGER, user_login TEXT, json JSON)")
        last_id = con.execute(f"SELECT MAX(id) FROM {self.table_name}").fetchone()[0] 

        if overwrite and self.table_name in get_all_tables(con):
            if last_id is not None:
                if await _ask_yes_no('Found existing table data on disk, do you want to overwrite all ? (y/n)'):
                    con.execute(f"CREATE OR REPLACE TABLE {self.table_name} (id INTEGER, user_login TEXT, json JSON)")
                    last_id = None

        # Create table in db s
        observers = await self._get_observers(con)
        
        #Start from previously saved pipe:

        if last_id is None:
            #Optionnal limit
            if limit is not None:
                observers = observers[:limit]
            
            self.obs_count = len(observers)
        else:
            logger.info(f"Last processed user : {observers[last_id]} with id : {last_id}")
            #Optionnal limit
            if limit is None:
                observers = observers[last_id:]
                self.obs_count = len(observers)
            else:
                observers = observers[last_id:limit+last_id]
                self.obs_count = limit+last_id
        
        #Store count of observers (for logger)
        
        async with aiohttp.ClientSession() as session:
            writer_task = asyncio.create_task(self._write_data(con, logger))
            
            fetchers = [asyncio.create_task(self._fetch_data(session, idx, o, logger)) for idx, o in observers ]
            await asyncio.gather(*fetchers)
            await self.queue.join()
            await self.queue.put(None)
            await writer_task
        
        step.storage['db'] = self.table_name
        step.status = StepStatus.completed

        
    async def _write_data(self, con, logger):
        logger.info('Init writer task')
        queue = self.queue

        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            idx, data = item
            con.execute(f"INSERT INTO {self.table_name} VALUES (?, ?, ?)", (idx, data['login'],json.dumps(data)))
            logger.info(f"Data saved for {data['login']} ({idx+1}/{self.obs_count})")
            queue.task_done()
        con.close()
        
    async def _fetch_data(self,session, idx:int, user_login:str, logger ):
        url = f"https://api.inaturalist.org/v1/users/autocomplete/?q={user_login}"
        
        async with self.limiter:
            try:
                async with session.get(url, timeout = 10) as r:
                    r.raise_for_status()
                    data = await r.json()    
                    if data:
                        await self.queue.put((idx,data["results"][0]))
            except Exception as e:
                logger.warning(f"[{user_login}] failed: {e}")
    async def _get_observers(self,con)->list[tuple]:
        query = """
                SELECT DISTINCT occurrenceID,
                FROM preprocessed.gbif_citizen
                WHERE institutionCode = 'iNaturalist'
                ORDER BY recordedBy ASC;
        """
        observers = con.execute(query).df()['recordedBy'].to_list()
        observers_tuples = []
        for idx, obs in enumerate(observers):
            observers_tuples.append((idx+1,obs))
        return observers_tuples

class iNatObsLoader(BaseLoader):
    def __init__(self, name:str):
        super().__init__()
        self.name = name

    async def run(self, pipe:Pipeline, step: PipelineStep):
        limit = pipe.config['inat_api']['limit']
        overwrite = pipe.config['inat_api']['overwrite']
        con = pipe.con
        logger = pipe.logger
        self.limiter = AsyncLimiter(pipe.config['inat_api']['max_calls_per_minute'], 60)
        step_name = self.name
        self.queue  = Queue()
        self.table_name = "raw.inat_observers"
        if step.status == StepStatus.completed:
            logger.info(f"{step_name} already completed")
            #SKip 
            return self.table_name
         
        
        # Create table for data
        con.execute(f"CREATE TABLE IF NOT EXISTS  {self.table_name} (id INTEGER, user_login TEXT, json JSON)")
        last_id = con.execute(f"SELECT MAX(id) FROM {self.table_name}").fetchone()[0] 

        if overwrite and self.table_name in get_all_tables(con):
            if last_id is not None:
                if await _ask_yes_no('Found existing table data on disk, do you want to overwrite all ? (y/n)'):
                    con.execute(f"CREATE OR REPLACE TABLE {self.table_name} (id INTEGER, user_login TEXT, json JSON)")
                    last_id = None

        # Create table in db s
        observers = await self._get_observers(con)
        
        #Start from previously saved pipe:

        if last_id is None:
            #Optionnal limit
            if limit is not None:
                observers = observers[:limit]
            
            self.obs_count = len(observers)
        else:
            logger.info(f"Last processed user : {observers[last_id]} with id : {last_id}")
            #Optionnal limit
            if limit is None:
                observers = observers[last_id:]
                self.obs_count = len(observers)
            else:
                observers = observers[last_id:limit+last_id]
                self.obs_count = limit+last_id
        
        #Store count of observers (for logger)
        
        async with aiohttp.ClientSession() as session:
            writer_task = asyncio.create_task(self._write_data(con, logger))
            
            fetchers = [asyncio.create_task(self._fetch_data(session, idx, o, logger)) for idx, o in observers ]
            await asyncio.gather(*fetchers)
            await self.queue.join()
            await self.queue.put(None)
            await writer_task
        
        step.storage['db'] = self.table_name
        step.status = StepStatus.completed

        
    async def _write_data(self, con, logger):
        logger.info('Init writer task')
        queue = self.queue

        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            idx, data = item
            con.execute(f"INSERT INTO {self.table_name} VALUES (?, ?, ?)", (idx, data['login'],json.dumps(data)))
            logger.info(f"Data saved for {data['login']} ({idx+1}/{self.obs_count})")
            queue.task_done()
        con.close()
        
    async def _fetch_data(self,session, idx:int, user_login:str, logger ):
        url = f"https://api.inaturalist.org/v1/users/autocomplete/?q={user_login}"
        
        async with self.limiter:
            try:
                async with session.get(url, timeout = 10) as r:
                    r.raise_for_status()
                    data = await r.json()    
                    if data:
                        await self.queue.put((idx,data["results"][0]))
            except Exception as e:
                logger.warning(f"[{user_login}] failed: {e}")
    async def _get_observers(self,con)->list[tuple]:
        query = """
                SELECT DISTINCT occurrenceID,
                FROM preprocessed.gbif_citizen
                WHERE institutionCode = 'iNaturalist'
                ORDER BY recordedBy ASC;
        """
        observers = con.execute(query).df()['recordedBy'].to_list()
        observers_tuples = []
        for idx, obs in enumerate(observers):
            observers_tuples.append((idx+1,obs))
        return observers_tuples

class inatApiLoader(BaseLoader):
    def __init__(self, pipe:Pipeline, name:str):
        self.limit = pipe.config['inat_api']['limit']
        self.overwrite = pipe.config['inat_api']['overwrite']
        self.limiter = AsyncLimiter(pipe.config['inat_api']['max_calls_per_minute'], 60)
        self.queue  = Queue()
        self.table_name = f"raw.{name}"

        super().__init__()
        self.name = name

    async def run(self, pipe:Pipeline, step: PipelineStep, items:list, endpoint:str, fields:str,):
        limit = pipe.config['inat_api']['limit']
        con = pipe.con
        logger = pipe.logger
        if step.status == StepStatus.completed:
            logger.info(f"{self.name} already completed")
            #SKip 
            return self.table_name
         
        # Create table for data
        con.execute(f"CREATE TABLE IF NOT EXISTS  {self.table_name} (id INTEGER, key TEXT, json JSON)")
        last_id = con.execute(f"SELECT MAX(id) FROM {self.table_name}").fetchone()[0] 

        if self.overwrite and self.table_name in get_all_tables(con):
            if last_id is not None:
                if await _ask_yes_no(f'Found existing table data on disk for {self.table_name}, do you want to overwrite all ? (y/n)'):
                    con.execute(f"CREATE OR REPLACE TABLE {self.table_name} (id INTEGER, key TEXT, json JSON)")
                    last_id = None
        
        #Start from previously saved pipe:

        if last_id is None:
            #Optionnal limit
            if limit is not None:
                items = items[:limit]
            
            self.item_count = len(items)
        else:
            logger.info(f"Last processed item : {items[last_id]} with id : {last_id}")
            #Optionnal limit
            if limit is None:
                items = items[last_id:]
                self.item_count = len(items)
            else:
                items = items[last_id:limit+last_id]
                self.item_count = limit+last_id
        
        #Store count of observers (for logger)
        async with aiohttp.ClientSession() as session:
            writer_task = asyncio.create_task(self._write_data(con, logger))
            
            fetchers = [asyncio.create_task(self._fetch_data(session, endpoint, fields, key, idx, logger)) for idx, key in enumerate(items) ]
            await asyncio.gather(*fetchers)
            await self.queue.join()
            await self.queue.put(None)
            await writer_task
        
        step.storage['db'] = self.table_name
        step.status = StepStatus.completed

        
    async def _write_data(self, con, logger):
        logger.info('Init writer task')
        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor(max_workers=1)
        processed_count = 0

        while True:
            item = await self.queue.get()
            logger.debug(f"Writer got item from queue, is None: {item is None}, queue size: {self.queue.qsize()}")
            if item is None:
                logger.info(f'Writer done, processed {processed_count} items')
                self.queue.task_done()
                break
            idx, data = item
            try:
                # Run blocking database call in thread pool to avoid blocking event loop
                def db_insert():
                    con.execute(
                        f"INSERT INTO {self.table_name} VALUES (?, ?, ?)",
                        (idx, data['id'], json.dumps(data))
                    )
                
                await loop.run_in_executor(executor, db_insert)
                processed_count += 1
                logger.debug(f'Inserted item {idx}')
                logger.info(f"Data saved for {data['id']} ({idx+1}/{self.item_count})")

            except Exception as e:
                logger.error(f"Failed to insert data for idx {idx}: {e}")
            
            self.queue.task_done()
        executor.shutdown(wait=True)
        
    async def _fetch_data(self,session, endpoint:str, fields:str, key:int, idx:int, logger ):
        url = f"https://api.inaturalist.org/v2/{endpoint}"
        params = {'id' : str(key),
                  "fields" : fields}
        async with self.limiter:
            try:
                async with session.get(url, params = params, timeout = 10) as r:
                    r.raise_for_status()
                    data = await r.json()    
                    if data:
                        try:
                            await self.queue.put((idx,data["results"][0]))
                        except Exception as e:
                            logger.error(e)
            except Exception as e:
                logger.error(f" {params['id']} failed: {e}")



# Convert to the special syntax
def fields_to_string(fields_dict, level=0):
    parts = []
    for key, value in fields_dict.items():
        if isinstance(value, dict):
            nested = fields_to_string(value, level + 1)
            parts.append(f"{key}:({nested})")
        elif value is True:
            parts.append(f"{key}:!t")
    return ','.join(parts)