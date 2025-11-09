from .data.factory import create_query
from .core import read_config, write_config, PipelineData
from .data.get_data import get_gbif
from .utils.debug import launch_debugger
from .utils.duckdb import _open_connection

from pathlib import Path
import argparse
import subprocess
import asyncio
from pprint import pprint
import logging
import shutil

    
def main():
    
    parser = argparse.ArgumentParser(
                    prog='BioCity',
                    description='What the program does',
                    epilog='Text at the bottom of help'
    )
    
    parser.add_argument("--file", "-f",help = 'Config File')
    parser.add_argument("--dev", "-d", action= 'store_true', help = 'Run dev')
    parser.add_argument("--debug", action= 'store_true', help = 'Run debugger')
    parser.add_argument("--force", action= 'store_true', help = 'Force re-run')

    args = parser.parse_args()
    
    # Debugger
    if args.debug:
        launch_debugger()
    
    # Check if required file, else try dev mode
    if not args.file:
        if not args.dev:
            raise UserWarning("Missing config file")
        #dev branch
        config_path = Path(__file__).parent.parent.parent / "work/dev/config.yaml"
    else:
        config_path = args.file
        
    #COnfig as dict
    config = read_config(config_path)
    
    folders = {}
    run_folder = Path(config_path).parent
    data_folder = (run_folder / 'data')
    pipe_folder = (data_folder / 'pipeline')
    db_path = data_folder / 'data.duckdb'
    
    #Set folder paths to config
    folders["run_folder"] = str(run_folder)
    folders["data_folder"] = str(data_folder)
    folders["pipeline_folder"] = str(pipe_folder)
    config['folders'] = folders
    
    # Force flag to wipe data
    if args.force:
        if data_folder.exists():
            shutil.rmtree(str(config_path.parent / 'data'))

    #New instance if totally new run (or forced)
    if not data_folder.exists():
        logging.info("No pipe data found, creating new instance from scratch")
        data = PipelineData(config = config)
        
        # Create folders 
        data_folder.mkdir(exist_ok= True)
        pipe_folder.mkdir(exist_ok= True)
        
    else:
        #Read from disk 
        try:
            data = (PipelineData(config= config,
                                 storage = read_config(pipe_folder / 'pipe_data.yaml'),
                                 step_status= read_config(pipe_folder / 'pipe_steps.yaml')
                                 ))
            logging.info("Previous pipe data found, creating new instance from data on disk")

        except Exception as e:
            raise Exception(e)


            
    asyncio.run(get_gbif(data))
    
    #asyncio.run(get_data(data))
    
    

    
    
    
    
    