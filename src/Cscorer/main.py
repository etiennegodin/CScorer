from .data.factory import create_query
from .core import read_config, write_config, PipelineData, StepStatus
from .data.main import get_data
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
    
    if args.debug:
        launch_debugger()
    

    if not args.file:
        if not args.dev:
            raise UserWarning("Missing config file")
        #dev branch
        config_path = Path(__file__).parent.parent.parent / "work/dev/config.yaml"

    else:
        config_path = args.file
    
    config = read_config(config_path)
    
    run_folder = Path(config_path).parent
    data_folder = (run_folder / 'data')
    pipe_folder = (data_folder / 'pipeline')
    db_path = data_folder / 'data.duckdb'
    
    if args.force:
        if (config_path.parent / 'data').exists():
            shutil.rmtree(str(config_path.parent / 'data'))
    
    
    
    data_folder.mkdir(exist_ok= True)
    pipe_folder.mkdir(exist_ok= True)

    config["run_folder"] = str(run_folder)
    config["data_folder"] = str(data_folder)
    config["pipeline_folder"] = str(pipe_folder)
    
    #New instance if totally new run 
    if not (pipe_folder / 'pipe_data.yaml').exists():
        logging.info("No pipe data found, creating new instance from scratch")
        data = PipelineData(config = config,
                            db_con= _open_connection(db_path)
)
    else:
        #Read from disk 
        try:
            data = (PipelineData(config= config,
                                 storage = read_config(pipe_folder / 'pipe_data.yaml'),
                                 step_status= read_config(pipe_folder / 'pipe_steps.yaml'),
                                 db_con= _open_connection(db_path)
                                 
                                 ))
            logging.info("Previous pipe data found, creating new instance from data on disk")

        except Exception as e:
            raise Exception(e)

            
    asyncio.run(get_data(data))

    
    
    

    
    #asyncio.run(get_data(data))
    
    

    
    
    
    
    