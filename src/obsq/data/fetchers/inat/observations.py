import glob
from ....utils import import_csv_to_db
from ....pipeline import *
from ....db import * 

@step
async def import_inat_observations(context:PipelineContext):
    
    # Find csv file
    files = glob.glob(f"{context.config['paths']['inat_folder']}/*.csv")
    n_files = len(files)
    if n_files == 0:
        raise ValueError("Found no csv in inat")
    elif n_files > 1:
        raise ValueError("Found multiple csv in inat")
    else:
        table_name = import_csv_to_db(context.con, files[0],
                                      'raw',
                                      'inat_observations')

    return table_name

clean_inat_observations = SqlQuery('clean_inat_observations', query_name= "inat_clean_observations" )

preprocess_inat_observations_submodule = SubModule("preprocess_inat_observations", [import_inat_observations, clean_inat_observations])

