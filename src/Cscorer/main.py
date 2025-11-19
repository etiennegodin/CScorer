from .data.main import data_submodules
from .features.main import features_submodules
from .pipeline import Pipeline, PipelineModule, StepStatus
from .pipeline.yaml_support import read_config
from .pipeline.core import load_function
import yaml
from pathlib import Path
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
   
def init_folder(config:dict, config_path:Path)->dict:
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

    return config  
      
def init_pipeline(args, pipe_struct:dict)->Pipeline:
    
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
    #Create folder structure
    config = init_folder(config, config_path)
    folders = config['folders']
    pipe_folder = Path(folders['pipeline_folder'])
    
    # Force flag to wipe data
    if args.force:
        data_folder = Path(folders['data_folder'])
        pipe_folder = Path(folders['pipeline_folder'])
        if data_folder.exists():
            shutil.rmtree(str(data_folder))
        if pipe_folder.exists():
            shutil.rmtree(str(pipe_folder))
            
        pipe = Pipeline(config = config)
        pipe.logger.info("Forcing new instance from scratch")

    # Create folders 
    for folder in folders.values():
        Path(folder).mkdir(exist_ok= True)

    #Read from disk 
    else:
        # New instance if totally new run 
        if not (pipe_folder /'pipe.yaml').exists() :
            # Create instance 
            pipe = Pipeline(config = config)
            pipe.logger.info("No pipe data found, creating new instance from scratch")

        else:
            try:
                #pipe = Pipeline.from_yaml_file(pipe_folder/'pipe.yaml')
                pipe = yaml.load(open(pipe_folder/'pipe.yaml'), Loader=yaml.FullLoader)
                pipe_reconstructor(pipe)
                pipe.logger.info("Previous pipe data found, creating new instance from data on disk")

            except Exception as e:
                raise Exception(e)
    if args.module == 'full':
        for module, submodules in pipe_struct.items():
            func = load_function(f"Cscorer.{module}.main.{module}_submodules")

            module = PipelineModule(module, func = func)
            print(module)
            quit()
            pipe.add_module(module)
            for sub in submodules.keys():
                pass
        
    quit()
    #Add modules
    data_module = PipelineModule('data',  func = data_submodules)
    features_module = PipelineModule('features',  func = features_submodules)    
    
    pipe.add_module(data_module)
    pipe.add_module(features_module)
    
    

    pprint(pipe.modules)
        

    return pipe
