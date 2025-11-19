from .main import init_pipeline
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

# as meta config
steps = ["get_gbif_data",
         "get_inaturalist_occurence_data",
         "get_inaturalist_observer_data",
         "get_environmental_data"]

modules = ['data', 'features', 'model']

pipe_struct = {  
               "data" : {'loaders' : 'Load data to disk', 'preprocessors' : 'Clean and preprocess data'},
               "features" : {'observer' : 'Process observer features'}
    
}
def dynamic_argparse(subparsers:argparse.ArgumentParser, struct:dict):
    parsers = []
    
    for key, values in struct.items():
        #modules
        submodules = {}
        submodules['full'] = 'Run full module'
        
        for sub_key, sub_values in values.items():
            submodules[sub_key] = sub_values
        
        module_parser = subparsers.add_parser(key, help = f"Run {key} module")
        module_subparser = module_parser.add_subparsers(title = f"{str(key).capitalize()} Module", dest='submodule', required = True,  description= f"Submodules available:", help= 'Subodules options')
        
        #submodules
        for sub_name, description in submodules.items():
            submodule_parser = module_subparser.add_parser(sub_name, help = description)
            submodule_parser.add_argument("--force", action= 'store_true', help = 'Force re-run')
        
    

def main():
    
    parser = argparse.ArgumentParser(
                    prog='Cscorer',
                    description='Citizen Science Observation Quality Scorer',
                    epilog='Text at the bottom of help'
    )
    

    subparsers = parser.add_subparsers(title= "Module", dest= 'module', required =True, description= "Modules of pipeline to run", help= 'Modules options')
    subparsers.add_parser('full', help = "Run full pipeline")
    dynamic_argparse(subparsers, pipe_struct)
    parser.add_argument("--file", "-f",help = 'Config File')
    parser.add_argument("--dev", "-d", action= 'store_true', help = 'Run dev')
    parser.add_argument("--debug", action= 'store_true', help = 'Run debugger')
    args = parser.parse_args()
    print(args)
    quit()
    # Debugger
    if args.debug:
        launch_debugger()
    
    # Init pipeline 
    pipe = init_pipeline(args)
    
    #Add modules
    data_module = PipelineModule('data',  func = data_submodules)
    features_module = PipelineModule('features',  func = features_submodules)    
    
    pipe.add_module(data_module)
    pipe.add_module(features_module)
    
    

    pprint(pipe.modules)
        
    asyncio.run(pipe.run())


           
        
    
    # eda 

    
    
    
    
    