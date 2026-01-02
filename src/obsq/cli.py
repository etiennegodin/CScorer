import argparse
from pathlib import Path
import os
import importlib.util
import logging
from .utils.debug import launch_debugger
from . import data, model

WORK_FOLDER = Path(os.getcwd())
ROOT_FOLDER = Path(__file__).resolve().parents[0] 

def dynamic_pipe_argparse(subparsers:argparse.ArgumentParser, struct:dict, global_parser):
    raise NotImplementedError("dynamic_pipe_argparse not implemented")

    parsers = []
    
    for key, values in struct.items():
        #modules
        submodules = {}
        submodules['full'] = 'Run full module'
        
        for sub_key, sub_values in values.items():
            submodules[sub_key] = sub_values
        
        module_parser = subparsers.add_parser(key, help = f"Run {key} module. Subdmodules: {[sub for sub in values.keys() ]}", parents=[global_parser])
        module_subparser = module_parser.add_subparsers(title = f"{str(key).capitalize()} Module", dest='submodule', required = True,  description= f"Submodules available:", help= 'Submodules options')
        
        #submodules
        for sub_name, description in submodules.items():
            submodule_parser = module_subparser.add_parser(sub_name, help = description, parents=[global_parser])

def main():
    
    global_parser = argparse.ArgumentParser(add_help = False)
    global_parser.add_argument("step", choices= ['data_ingest','data_prep','model'], help = 'Pipeline step')
    global_parser.add_argument("--from_module", "-f", help = "Start from this module (inclusive)")
    global_parser.add_argument("--to_module", "-t", help = "Stop at this module (inclusive)" )
    global_parser.add_argument("--only_modules", "-o", help = "Run only these modules (list of module names)")
    global_parser.add_argument("--skip_modules","-s", help = "Skip these modules (list of module names)")
    global_parser.add_argument("--force", action= 'store_true', help = "Ignore last checkpoint")

    parser = argparse.ArgumentParser(
                    prog='obsq',
                    description='Citizen Science Observation Quality Scorer',
                    epilog='Text at the bottom of help',
                    parents=[global_parser]
    )
    

    """
    pipe_config = read_config(work_folder / "pipe_config.yaml")
    pipe_struct = pipe_config['pipe_struct']
    subparsers = parser.add_subparsers(title= "Module", dest= 'module', required =True, description= "Modules of pipeline to run", help= 'Modules options')
    subparsers.add_parser('full', help = "Run full pipeline", parents=[global_parser])
    dynamic_pipe_argparse(subparsers, pipe_struct, global_parser)
    """
    
    args = parser.parse_args()


    # Run main of current pipeline    
    if "config.yaml" in os.listdir(WORK_FOLDER):


        if args.step == "data_ingest":
            to_run = data.ingest.run
        elif args.step == "data_prep":
            to_run = data.prep.run
        elif args.step == "model":
            to_run = model.run

        to_run(ROOT_FOLDER, WORK_FOLDER, args)
        
    else:
        logging.error(f" No 'config file found in {WORK_FOLDER}")


if __name__ == "__main__":
    main()