from .base import BaseQuery
from shapely import wkt
from ..core import PipelineData, StepStatus
from ..utils.duckdb import import_csv_to_db, get_all_tables
import asyncio, aiohttp, aiofiles
from aiolimiter import AsyncLimiter
from asyncio import Queue
from pprint import pprint
import json

class iNatObs(BaseQuery):
    def __init__(self, name:str):
        super().__init__()
        self.name = name

    async def run(self, data:PipelineData, limit:int = None, overwrite:bool = False):
        con = data.con
        logger = data.logger
        self.limiter = AsyncLimiter(data.config['inat_api']['max_calls_per_minute'], 60)
        step_name = self.name
        self.queue  = Queue()
        table_name = "inat.observers"
         
        # Create table for data
        con.execute(f"CREATE TABLE IF NOT EXISTS  {table_name} (id INTEGER, user_login TEXT, json JSON)")
        last_id = con.execute(f"SELECT MAX(id) FROM {table_name}").fetchone()[0] 

        if overwrite and table_name in get_all_tables(con):
            if last_id is not None:
                if await _ask_yes_no('Found existing table data on disk, do you want to overwrite all ? (y/n)'):
                    con.execute(f"CREATE OR REPLACE TABLE {table_name} (id INTEGER, user_login TEXT, json JSON)")
                    last_id = None

        # Create table in db s
        observers = await _get_observers(con)
        
        #Start from previously saved data:

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
        
        data.update_step_status(step_name, StepStatus.completed)

        
    async def _write_data(self, con, logger):
        logger.info('Init writer task')
        queue = self.queue

        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            idx, data = item
            con.execute("INSERT INTO inat.observers VALUES (?, ?, ?)", (idx, data['login'],json.dumps(data)))
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

class iNatOcc(BaseQuery):

    def __init__(self, name:str):
        super().__init__()
        self.name = name

    async def run(self, data:PipelineData):
        import webbrowser
        logger = data.logger
        step_name = self.name
        inat_folder = data.config['folders']['inat_folder']

        
        if "query" not in data.storage[step_name].keys():
            raise NotImplementedError("Query builder not implemented")
            query = await self._build_query()
            data.storage[step_name]['query'] = query
            data.update()
        else:
            pass
            query = data.storage[step_name]['query']
            
        #Override query
        query = "has%5B%5D=photos&quality_grade=research&identifications=any&captive=false&swlat=45.014526+&swlng=-74.519611&nelat=46.821866+&nelng=-70.203212&not_in_place=187355&taxon_id=211194&d1=2021-01-01&d2=2025-11-01"
        url = f"https://www.inaturalist.org/observations/export?{query}"
        
        if data.step_status[f'{step_name}'] == StepStatus.init:
            webbrowser.open(url)
            data.update_step_status(step_name, StepStatus.requested)
            input(f"Please save requested csv file to target directory and relaunch \n -Target dir: {inat_folder}")
            
        if data.step_status[f'{step_name}'] == StepStatus.requested:
            logger.info(" Trying to find csv to merge in db from inat data folder")
            try:
                files = list(inat_folder.glob('**/*.csv')) 
            except Exception as e:
                logger.error(e)
                
            if not files:
                raise ImportError(f"No export files availabe in {inat_folder} - Please relaunch with data")
            
            data.step_status[f'{step_name}'] == StepStatus.local
            
        if data.step_status[f'{step_name}'] == StepStatus.local:
            logger.info(f"Found {len(files)} files")
            if len(files) > 1:
                if not await _ask_yes_no('Found multiples files, do you want to process all ? (y/n)'):
                    new_files = []
                    lines = ""
                    for idx, f in enumerate(files):
                        lines += (f"\n{idx} : {f.stem}.{f.suffix}")

                    file_index = await _ask_file_input(len(files), lines)
                    new_files.append(files[file_index])
                    
            occ_tables = []
            for f in files:
                occ_table = await import_csv_to_db(data.con,f,'inat','occurences', replace= False)
                occ_tables.append(occ_table)
                
            if occ_tables == len(files):
                data.update_step_status(step_name, StepStatus.completed)
                return occ_tables[0] # assuming same table with appended data if multiples sources
                
    async def _build_query(self):
        pass

    def _get_bounds(config):
        geo = config['gbif']['GEOMETRY']
        geom = wkt.loads(geo)
        minx, miny, maxx, maxy = geom.bounds

        # South-West and North-East
        lat_so, lon_so = miny, minx
        lat_ne, lon_ne = maxy, maxx

        print(lat_so, lon_so )
        print(lat_ne, lon_ne)
    
async def _ask_yes_no(msg:str):
    values = ['y','n']
    correct = False
    while (not correct):
        answer = input(msg)
        if answer.lower() in values:
            correct = True
    
    if answer == 'y':
        return True
    else:
        return False

async def _ask_file_input(n_inputs, lines):
    correct = False
    while (not correct):
        file_index = input(f"""Please choose file from list{lines}\nAnswer: """)
        try:
            file_index = int(file_index)
            if (file_index + 1) <= n_inputs:
                correct = True
        except:
            print ("\033[A                             \033[A")
            
    return file_index
    

async def _get_occurenceIDs(con):
    query = """
            SELECT occurrenceID,
            FROM gbif_raw.citizen
            WHERE institutionCode = 'iNaturalist';
    """
    
    occurenceURLs = con.execute(query).df()['occurrenceID']
    #occurenceIDs = occurenceURLs.apply(lambda x: x.split(sep='/')[-1]).to_list()
    
    return occurenceURLs.to_list()


async def _get_observers(con)->list[tuple]:
    query = """
            SELECT DISTINCT recordedBy,
            FROM gbif_raw.citizen
            WHERE institutionCode = 'iNaturalist'
            ORDER BY recordedBy ASC;
    """
    observers = con.execute(query).df()['recordedBy'].to_list()
    observers_tuples = []
    for idx, obs in enumerate(observers):
        observers_tuples.append((idx+1,obs))
    return observers_tuples