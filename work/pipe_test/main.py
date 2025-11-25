from pathlib import Path
from obsq.utils import read_config, create_folders
from obsq.pipeline import Pipeline
from obsq.modules import * 


def main(root_folder, args):

    #Config as dict
    try:
        config = read_config(root_folder / args.config)
    except FileNotFoundError:
        raise FileNotFoundError("Please provide a valid config file")
    
    #Create folder structure
    config["paths"] = create_folders(root_folder)
    print('main_func')
    
    pipeline = Pipeline('pipe_test', [myModule], root_folder/ "checkpoints")
    
    pipeline.run(config, resume_from_checkpoint= True)
    
    
if __name__ == "__main__":
    print('main')