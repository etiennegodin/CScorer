import glob
from ...utils import import_csv_to_db
from ...pipeline import *
from ...steps import * 

@step
async def import_inat_observations(context:PipelineContext):
    
    # Find csv file
    
    files = glob.glob(f"{context.config['paths']['inat_folder']}/*.csv")
    if len(files) == 1:
        table_name = import_csv_to_db(context.con, files[0],
                                      'raw',
                                      'inat_observations')
    else:
        raise ValueError("Found multiple csv in inat")
    return table_name

clean_inat_observations = SimpleQuery('clean_inat_observations', query_name= "inat_clean_observations" )

preprocess_inat_observations_submodule = SubModule("preprocess_inat_observations", [import_inat_observations, clean_inat_observations])

