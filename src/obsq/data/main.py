from ..utils import *
from ..pipeline import *

def run(root_folder, work_folder, args):
    print('prut')
    print(root_folder)

def run_old(root_folder, work_folder, args):
    resume = True
    if args.force:
        resume = False
    #Config as dict
    try:
        config = read_config(work_folder / args.config)
    except Exception as e:
        raise ("Please provide a valid config file")
    
    # Create folder structure
    config["paths"] = create_folders(root_folder, work_folder)
      
    pipeline = Pipeline('pipe_test', [m.db_init, m.gbif_ingest,
                                      m.gbif_preprocess,
                                      m.inat_data,
                                      #spatial data,
                                      m.label_data,
                                      m.extractor_features],
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