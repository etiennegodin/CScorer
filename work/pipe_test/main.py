from pathlib import Path
from obsq.utils import read_config, create_folders
from obsq.pipeline import Pipeline, Module
from obsq.modules import gbif_ingest_module
from obsq.steps import DataBaseConnection

def main(ROOT_FOLDER, work_folder, args):
    resume = True
    if args.force:
        resume = False
    #Config as dict
    try:
        config = read_config(work_folder / args.config)
    except FileNotFoundError:
        raise FileNotFoundError("Please provide a valid config file")
    
    
    # Create folder structure
    config["paths"] = create_folders(ROOT_FOLDER, work_folder)
      
    #Create init module with db connection 
    db_connection = DataBaseConnection(config['paths']['db_path'])
    init = Module('init', [db_connection], always_run= True)

    pipeline = Pipeline('pipe_test', [init, gbif_ingest_module], work_folder/ "checkpoints")
    
    #pipeline = Pipeline('pipe_test', [init], work_folder/ "checkpoints_test")
    pipeline.run(config, resume_from_checkpoint = resume)

    
if __name__ == "__main__":
    print('main')