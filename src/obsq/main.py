from .pipeline import Pipeline, PipelineModule, PipelineSubmodule, StepStatus
from .pipeline.yaml_support import read_config
from .pipeline.core import load_function
import obsq # to reload functions from string
import yaml
from pathlib import Path
from pprint import pprint
import shutil
import asyncio

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

def create_folders(folders:dict):
    # Create folders 
    for folder in folders.values():
        Path(folder).mkdir(exist_ok= True)
        
def build_full_pipeline(args, pipe:"Pipeline", pipe_struct)->dict:
    to_run = {}
    for module_name, submodules in pipe_struct.items():
        mod_func = load_function(f"obsq.{module_name}.main.{module_name}_submodules")
        module = PipelineModule(module_name, func = mod_func)
        submodules_list = []
        for submodule_name in submodules.keys():
            sub_func = load_function(f"obsq.{module_name}.{submodule_name}.main.{module_name}_{submodule_name}")
            submodule = PipelineSubmodule(submodule_name, func= sub_func)
            submodules_list.append(submodule)
            module.add_submodule(submodule, args.force)
        #Add back to pipe once all submodules are declaed
        pipe.add_module(module, args.force)
        to_run[module.name] = submodules_list 
    return to_run
        
def build_full_module(args, pipe:"Pipeline", pipe_struct:dict)->dict:
    """_summary_

    Args:
        args (_type_): _description_
        pipe (Pipeline): _description_
        pipe_struct (dict): _description_

    Returns:
        dict: _description_
    """
    to_run = {}
    module_name = args.module
    submodules_name = args.submodule
    
    if submodules_name == "full":
        submodules = pipe_struct[module_name].keys() # get all submodules 
        mod_func = load_function(f"obsq.{module_name}.main.{module_name}_submodules")
        module = PipelineModule(module_name, func = mod_func)
        submodules_list = []
        for submodule_name in submodules:
            sub_func = load_function(f"obsq.{module_name}.{submodule_name}.main.{module_name}_{submodule_name}")
            submodule = PipelineSubmodule(submodule_name, func= sub_func)
            submodules_list.append(submodule)
            module.add_submodule(submodule, args.force)

        #Add back to pipe once all submodules are declaed
        pipe.add_module(module, args.force)
        to_run[module.name] = submodules_list
    return to_run
    
    
def build_full_submodule(args, pipe:Pipeline, pipe_struct:dict)->dict:
    """
    Return a list of random ingredients as strings.

    :param kind: Optional "kind" of .
    :type kind: list[str] or None
    :raise lumache.InvalidKindError: If the kind is invalid.
    :return: The ingredients list.
    :rtype: list[str]

    """
    to_run = {}
    module_name = args.module
    submodule_name = args.submodule
    submodules = [] #need to be a list even if single submodule
    
    mod_func = load_function(f"obsq.{module_name}.main.{module_name}_submodules")
    module = PipelineModule(module_name, func = mod_func)
    submodules_list = []

    sub_func = load_function(f"obsq.{module_name}.{submodule_name}.main.{module_name}_{submodule_name}")
    submodule = PipelineSubmodule(submodule_name, func= sub_func)
    submodules_list.append(submodule)
    module.add_submodule(submodule, args.force)

    #Add back to pipe once all submodules are declaed
    pipe.add_module(module, args.force)
    to_run[module.name] = submodules_list
    return to_run
   
def run_pipeline(args, pipe_struct:dict)->Pipeline:
    
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
    try:
        config = read_config(config_path)
    except FileNotFoundError:
        raise FileNotFoundError("Please provide a valid config file")

    #Create folder structure
    config = init_folder(config, config_path)
    folders = config['folders']
    pipe_folder = Path(folders['pipeline_folder'])
    
    # Force flag to wipe data
    if args.force and args.module == 'full':
        data_folder = Path(folders['data_folder'])
        pipe_folder = Path(folders['pipeline_folder'])
        if data_folder.exists():
            shutil.rmtree(str(data_folder))
        if pipe_folder.exists():
            shutil.rmtree(str(pipe_folder))
            
        # Re-create folders
        create_folders(folders)        
        pipe = Pipeline(config = config)
        pipe.logger.info("Forcing new instance from scratch")
        
    #Read from disk 
    else:
        # New instance if totally new run 
        if not (pipe_folder /'pipe.yaml').exists() :
            # Create instance 
            create_folders(folders)
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
        to_run = build_full_pipeline(args, pipe, pipe_struct)

    else:
        if args.submodule == "full":
            to_run = build_full_module(args, pipe, pipe_struct)
        else:
            to_run = build_full_submodule(args, pipe, pipe_struct)

    asyncio.run(pipe.run(to_run, args.force))
