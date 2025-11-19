from .data.main import data_submodules
from .features.main import features_submodules
from .utils.debug import launch_debugger
from .pipeline import Pipeline, PipelineModule, StepStatus
from .pipeline.yaml_support import read_config

import yaml
from pathlib import Path
import argparse
import subprocess
import asyncio

from pprint import pprint
import logging
import shutil


def pipe_reconstructor(pipeline:Pipeline):
    for module in pipeline.modules.values():
        module._parent = pipeline
        for sub in module.submodules.values():
            sub._parent = module
            for step in sub.steps.values():
                step._parent = sub
                
def init_pipeline(args)->Pipeline:
    
    # Check if required file, else try dev mode
    if not args.file:
        if not args.dev:
            raise UserWarning("Missing config file")
        #dev branch
        config_path = Path(__file__).parent.parent.parent / "work/dev/config.yaml"
        #config_path = Path(__file__).parent.parent.parent / "work/pipe_test/config.yaml"

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
    eda_folder = data_folder/ 'eda'

    db_path = data_folder / 'data.duckdb'

    #Set folder paths to config
    folders["run_folder"] = str(run_folder)
    folders["pipeline_folder"] = str(pipe_folder)

    folders["data_folder"] = str(data_folder)
    folders["eda_folder"] = str(eda_folder)

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
        pipe = Pipeline(config = config)

    #Read from disk 
    else:
        try:
            #pipe = Pipeline.from_yaml_file(pipe_folder/'pipe.yaml')
            pipe = yaml.load(open(pipe_folder/'pipe.yaml'), Loader=yaml.FullLoader)
            pipe_reconstructor(pipe)
            logging.info("Previous pipe data found, creating new instance from data on disk")

        except Exception as e:
            raise Exception(e)

    return pipe
