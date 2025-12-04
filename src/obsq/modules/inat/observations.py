import glob
from ...utils import import_csv_to_db
from ...pipeline import *
from ...steps import * 

def import_inat_observations(context:PipelineContext):
    
    # Find csv file
    
    files = glob.glob(f"{context.config['paths']['inat_folder']}/*.csv")
    if len(files) == 1:
        import_csv_to_db(context.con, files, 'raw', 'inat_observations')
    

DataBaseQuery

inat_observations = SubModule("inat_observations", [import_inat_observations])
