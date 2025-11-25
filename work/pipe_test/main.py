from pathlib import Path
from obsq.utils import read_config, create_folders
from obsq.pipeline import Pipeline, Module
from obsq.modules import myModule, m_collect_gbif
from obsq.steps import DataBaseConnection

def main(root_folder, args):

    #Config as dict
    try:
        config = read_config(root_folder / args.config)
    except FileNotFoundError:
        raise FileNotFoundError("Please provide a valid config file")
    
    # Create folder structure
    config["paths"] = create_folders(root_folder)
      
    #Create init module with db connection 
    db_connection = DataBaseConnection(config['paths']['db_path'])
    init = Module('init', [db_connection])

    pipeline = Pipeline('pipe_test', [init, m_collect_gbif,myModule], root_folder/ "checkpoints")
    
    #pipeline.run(config, resume_from_checkpoint= False)
    pipeline.run(config, resume_from_checkpoint= False)

    
if __name__ == "__main__":
    print('main')