from .data.factory import create_query
from .core import read_config, PipelineData
from .data.main import get_data
from pathlib import Path
import argparse
import subprocess
import asyncio
import os
def main():
    
    parser = argparse.ArgumentParser(
                    prog='BioCity',
                    description='What the program does',
                    epilog='Text at the bottom of help'
    )
    
    parser.add_argument("--file", "-f",help = 'Config File')
    parser.add_argument("--dev", "-d", action= 'store_true', help = 'Run dev')
    parser.add_argument("--debug", action= 'store_true', help = 'Run debugger')

    args = parser.parse_args()
    
    if not args.file:
        if not args.dev:
            raise UserWarning("Missing config file")
        config_path = Path(__file__).parent.parent.parent / "work/dev/config.yaml"
        
    else:
        config_path = args.file
    
    config = read_config(config_path)
    
    run_folder = Path(config_path).parent
    data_folder = run_folder / 'data'
    config_folder = run_folder / 'configs'

    if not (data_folder / 'pipe_data.yaml').exists():
        data = PipelineData(config = config)

    data = (PipelineData(config= config, storage = read_config(data_folder / 'pipe_data.yaml')))
    
    data.set_config("run_folder", run_folder)
    data.set_config("data_folder", run_folder)

    asyncio.run(get_data(data))

    
    
    

    
    #asyncio.run(get_data(data))
    
    

    
    
    
    
    