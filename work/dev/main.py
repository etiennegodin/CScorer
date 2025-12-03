from pathlib import Path
from obsq.utils import read_config, create_folders
from obsq.pipeline import Pipeline, Module
from obsq import modules as m

def main(ROOT_FOLDER, work_folder, args):
    resume = True
    if args.force:
        resume = False
    #Config as dict
    try:
        config = read_config(work_folder / args.config)
    except FileNotFoundError:
        raise FileNotFoundError("Please provide a valid config file")
    
    # Create folder structure
    config["paths"] = create_folders(ROOT_FOLDER, work_folder)
      
    pipeline = Pipeline('pipe_test', [m.db_init, m.gbif_ingest,
                                      m.gbif_preprocess,
                                      m.inat_data,
                                      m.all_features],
                                    work_folder/ "checkpoints", 
                                    config = config)
    
    #pipeline = Pipeline('pipe_test', [init], work_folder/ "checkpoints_test")
    pipeline.run(
                 resume_from_checkpoint = resume,
                 from_module= args.from_module,
                 to_module= args.to_module,
                 only_modules=args.only_modules,
                 skip_modules=args.skip_modules
                 )

    
if __name__ == "__main__":
    print('main')