from .data.loaders.factory import create_query
from .core import read_config, write_config, Pipeline

from .data.main import data_main
from .utils.debug import launch_debugger
from .utils.duckdb import _open_connection

from pathlib import Path
import argparse
import subprocess
import asyncio

from pprint import pprint
import logging
import shutil

# as meta config
steps = ["get_gbif_data",
         "get_inaturalist_occurence_data",
         "get_inaturalist_observer_data",
         "get_environmental_data"]

modules = ['data', 'features', 'model']

def init_pipeline(args)->Pipeline:
    
    # Check if required file, else try dev mode
    if not args.file:
        if not args.dev:
            raise UserWarning("Missing config file")
        #dev branch
        #config_path = Path(__file__).parent.parent.parent / "work/dev/config.yaml"
        config_path = Path(__file__).parent.parent.parent / "work/pipe_test/config.yaml"

    else:
        config_path = args.file
    
    #Config as dict
    config = read_config(config_path)
    
    folders = {}
    run_folder = Path(config_path).parent
    pipe_folder = (run_folder / 'pipeline')
    
    data_folder = (run_folder / 'data')
    gbif_folder = (data_folder / 'gbif')
    inat_folder = data_folder/ 'inat'
    gee_folder = data_folder/ 'gee'

    db_path = data_folder / 'data.duckdb'

    #Set folder paths to config
    folders["run_folder"] = str(run_folder)
    folders["pipeline_folder"] = str(pipe_folder)

    folders["data_folder"] = str(data_folder)
    folders["gbif_folder"] = str(gbif_folder)
    folders["inat_folder"] = str(inat_folder)
    folders["gee_folder"] = str(gee_folder)

    #Add to config dict
    config['folders'] = folders
    config['db_path'] = str(db_path)
    
    
    # Force flag to wipe data
    if args.force:
        if data_folder.exists():
            shutil.rmtree(str(data_folder))
        if pipe_folder.exists():
            shutil.rmtree(str(pipe_folder))
        
    # Create folders 
    for folder in folders.values():
        Path(folder).mkdir(exist_ok= True)

    #New instance if totally new run (or forced)
    if not (pipe_folder/'pipe.yaml').exists() :
        logging.info("No pipe data found, creating new instance from scratch")
        # Create instance 
        pipe_data = Pipeline(config = config)
        

    #Read from disk 
    else:
        try:
            pipe_data = Pipeline.from_yaml_file(pipe_folder/'pipe.yaml')
            logging.info("Previous pipe data found, creating new instance from data on disk")

        except Exception as e:
            raise Exception(e)

    return pipe_data


def main():
    
    parser = argparse.ArgumentParser(
                    prog='BioCity',
                    description='What the program does',
                    epilog='Text at the bottom of help'
    )
    
    parser.add_argument("--file", "-f",help = 'Config File')
    parser.add_argument("--module", choices=modules, help = "Optionnal run only one module")
    parser.add_argument("--step", choices=steps, help = "Optionnal run only one step")
    parser.add_argument("--dev", "-d", action= 'store_true', help = 'Run dev')
    parser.add_argument("--debug", action= 'store_true', help = 'Run debugger')
    parser.add_argument("--force", action= 'store_true', help = 'Force re-run')

    args = parser.parse_args()

    # Debugger
    if args.debug:
        launch_debugger()
    
    # Init pipeline 
    pipe = init_pipeline(args)
    
    

    data_main((pipe))
    
    
    
    # eda 

    
    
    
    
    