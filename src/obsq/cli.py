from .main import run_pipeline
from .utils.debug import launch_debugger
from .pipeline.yaml_support import read_config
import argparse
import asyncio

def dynamic_pipe_argparse(subparsers:argparse.ArgumentParser, struct:dict, global_parser):
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
    
    pipe_config = read_config("/home/manat/projects/CScorer/config.yaml")
    pipe_struct = pipe_config['pipe_struct']
    
    
    global_parser = argparse.ArgumentParser(add_help = False)
    global_parser.add_argument("--file", "-f",help = 'Config File')
    global_parser.add_argument("--dev", "-d", action= 'store_true', help = 'Run dev')
    global_parser.add_argument("--debug", action= 'store_true', help = 'Run debugger')
    global_parser.add_argument("--force", action= 'store_true', help = 'Force to run')

    parser = argparse.ArgumentParser(
                    prog='Cscorer',
                    description='Citizen Science Observation Quality Scorer',
                    epilog='Text at the bottom of help',
                    parents=[global_parser]
    )
    

    subparsers = parser.add_subparsers(title= "Module", dest= 'module', required =True, description= "Modules of pipeline to run", help= 'Modules options')
    subparsers.add_parser('full', help = "Run full pipeline", parents=[global_parser])
    dynamic_pipe_argparse(subparsers, pipe_struct, global_parser)
    args = parser.parse_args()
    # Debugger
    if args.debug:
        launch_debugger()
        
    print(args)
    
    # Init pipeline 
    pipe = run_pipeline(args, pipe_struct)

    #asyncio.run(pipe.run())
