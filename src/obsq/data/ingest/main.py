from ...utils import *
from ...pipeline import *
from ...db import * 
from pathlib import Path
from . import *

def run(root_folder:Path, work_folder:Path, args):

    resume = True
    if args.force:
        resume = False
    #Config as dict
    try:
        config = read_config(work_folder / 'config.yaml')
    except Exception as e:
        raise ("Please provide a valid config file")
    
    # Create folder structure
    config["paths"] = create_folders(root_folder, work_folder)
    
    pipeline = Pipeline('data_ingest', [db_init, ingest_gbif_module,
                                      preprocess_gbif_module,
                                      inat_observers_module,
                                      gbif_post_inat_observers_module,
                                      inat_species_module,
                                      inat_ranges_module
                                      
                                      #spatial data
                                       ],
                                    work_folder/ "checkpoints", 
                                    config = config)
    pipeline.run(
                 resume_from_checkpoint = resume,
                 from_module= args.from_module,
                 to_module= args.to_module,
                 only_modules=args.only_modules,
                 skip_modules=args.skip_modules
                 )
