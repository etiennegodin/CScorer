from ..utils import *
from ..pipeline import *
from ..db import * 
from .extract import extract_features_module
from .transform import transform_features_module
from .encode import encode_features_module
from .score import score_obs_module
from .combine import combine_features_module
from pathlib import Path

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
    
    pipeline = Pipeline('features', [db_init,
                                      extract_features_module,
                                      transform_features_module,
                                      encode_features_module,
                                      combine_features_module,
                                      score_obs_module],
                                    work_folder/ "checkpoints", 
                                    config = config)
    pipeline.run(
                 resume_from_checkpoint = resume,
                 from_module= args.from_module,
                 to_module= args.to_module,
                 only_modules=args.only_modules,
                 skip_modules=args.skip_modules
                 )
