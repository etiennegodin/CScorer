from pathlib import Path
from obsq.utils import read_config, create_folders

def main(root_folder, args):

    #Config as dict
    try:
        config = read_config(root_folder / args.config)
    except FileNotFoundError:
        raise FileNotFoundError("Please provide a valid config file")
    
    #Create folder structure
    config["paths"] = create_folders(root_folder)
    print('main_func')
    
if __name__ == "__main__":
    print('main')