from .main import init_pipeline
from .utils.debug import launch_debugger
from .pipeline.yaml_support import read_config
import argparse
import asyncio

def dynamic_pipe_argparse(subparsers:argparse.ArgumentParser, struct:dict):
    parsers = []
    
    for key, values in struct.items():
        #modules
        submodules = {}
        submodules['full'] = 'Run full module'
        
        for sub_key, sub_values in values.items():
            submodules[sub_key] = sub_values
        
        module_parser = subparsers.add_parser(key, help = f"Run {key} module. Subdmodules: {[sub for sub in values.keys() ]}")
        module_subparser = module_parser.add_subparsers(title = f"{str(key).capitalize()} Module", dest='submodule', required = True,  description= f"Submodules available:", help= 'Submodules options')
        
        #submodules
        for sub_name, description in submodules.items():
            submodule_parser = module_subparser.add_parser(sub_name, help = description)
            submodule_parser.add_argument("--force", action= 'store_true', help = 'Force re-run')


def main():
    
    pipe_config = read_config("/home/manat/projects/CScorer/config.yaml")
    pipe_struct = pipe_config['pipe_struct']
    
    parser = argparse.ArgumentParser(
                    prog='Cscorer',
                    description='Citizen Science Observation Quality Scorer',
                    epilog='Text at the bottom of help'
    )
    

    subparsers = parser.add_subparsers(title= "Module", dest= 'module', required =True, description= "Modules of pipeline to run", help= 'Modules options')
    subparsers.add_parser('full', help = "Run full pipeline")
    dynamic_pipe_argparse(subparsers, pipe_struct)
    parser.add_argument("--file", "-f",help = 'Config File')
    parser.add_argument("--dev", "-d", action= 'store_true', help = 'Run dev')
    parser.add_argument("--debug", action= 'store_true', help = 'Run debugger')
    args = parser.parse_args()

    # Debugger
    if args.debug:
        launch_debugger()
    print(args)
    
    quit()
    # Init pipeline 
    pipe = init_pipeline(args, pipe_config)

    asyncio.run(pipe.run())
