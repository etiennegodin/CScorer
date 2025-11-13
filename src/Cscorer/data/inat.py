from .base import BaseQuery
from shapely import wkt
from ..core import PipelineData, StepStatus
from pathlib import Path
from ..utils.duckdb import import_csv_to_db


class iNatObs(BaseQuery):
    def __init__(self, name:str):
        super().__init__()
        self.name = name

    async def run(self, data:PipelineData):
        con = data.con
        logger = data.logger
        step_name = self.name

        
        observers = _get_observers(con)

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
                if not await self._ask_yes_no('Found multiples files, do you want to process all ? (y/n)'):
                    new_files = []
                    lines = ""
                    for idx, f in enumerate(files):
                        lines += (f"\n{idx} : {f.stem}.{f.suffix}")

                    file_index = await self._ask_file_input(len(files), lines)
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
        
    async def _ask_yes_no(self, msg:str):
        values = ['y','n']
        correct = False
        while (not correct):
            answer = input(msg)
            if answer.lower() in values:
                correct = True
                return answer

    async def _ask_file_input(self, n_inputs, lines):
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


async def _get_observers(con):
    query = """
            SELECT DISTINCT recordedBy,
            FROM gbif_raw.citizen
            WHERE institutionCode = 'iNaturalist';
    """
    return con.execute(query).df()['recordedBy'].to_list()
