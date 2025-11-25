import argparse
from pathlib import Path
import os
import importlib.util
from .utils.debug import launch_debugger
from .utils.core import read_config
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
    global_parser.add_argument("config", help = 'Config File')
    global_parser.add_argument("--from_module", "-f", help = "Start from this module (inclusive)")
    global_parser.add_argument("--to_module", "-t", help = "Stop at this module (inclusive)" )
    global_parser.add_argument("--only_modules", "-o", help = "Run only these modules (list of module names)")
    global_parser.add_argument("--skip_modules","-s", help = "Skip these modules (list of module names)")
    global_parser.add_argument("--resume", "-r", action= 'store_true', help = "Resume from last checkpoint")
    global_parser.add_argument("--debug", action= 'store_true', help = 'Run debugger')

    parser = argparse.ArgumentParser(
                    prog='obsq',
                    description='Citizen Science Observation Quality Scorer',
                    epilog='Text at the bottom of help',
                    parents=[global_parser]
    )
    
    
    file = global_parser.parse_args().config
    root_folder = Path(file).resolve().parents[0] 

    """
    pipe_config = read_config(root_folder / "pipe_config.yaml")
    pipe_struct = pipe_config['pipe_struct']
    subparsers = parser.add_subparsers(title= "Module", dest= 'module', required =True, description= "Modules of pipeline to run", help= 'Modules options')
    subparsers.add_parser('full', help = "Run full pipeline", parents=[global_parser])
    dynamic_pipe_argparse(subparsers, pipe_struct, global_parser)
    """
    
    args = parser.parse_args()
    
    # Debugger
    if args.debug:
        launch_debugger()
        
    # Run main of current pipeline    
    if "main.py" in os.listdir(root_folder):
        spec=importlib.util.spec_from_file_location("pipe_main",f"{root_folder}/main.py")
        
        # creates a new module based on spec
        pipe_main = importlib.util.module_from_spec(spec)
        # executes the module in its own namespace
        # when a module is imported or reloaded.
        spec.loader.exec_module(pipe_main)
        
        pipe_main.main(root_folder, args)
