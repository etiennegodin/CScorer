from pathlib import Path


def create_folders(parent_folder:Path)->dict:
    """_summary_

    Args:
        config_path (Path): _description_

    Returns:
        dict: _description_
    """
    
    folders = {}

    queries_folder = (parent_folder / 'queries')

    data_folder = (parent_folder / 'data')
    gbif_folder = (data_folder / 'gbif')
    inat_folder = data_folder/ 'inat'
    gee_folder = data_folder/ 'gee'
    eda_folder = data_folder/ 'eda'
    db_path = data_folder / 'data.duckdb'

    # Set folder paths to config
    folders["root_folder"] = str(parent_folder)
    
    #folders["modules_folder"] = str(modules_folder)
    #folders["steps_folder"] = str(steps_folder)
    #folders["checkpoints_folder"] = str(checkpoints_folder)
    folders["queries_folder"] = str(queries_folder)

    folders["data_folder"] = str(data_folder)
    folders["eda_folder"] = str(eda_folder)
    folders["gbif_folder"] = str(gbif_folder)
    folders["inat_folder"] = str(inat_folder)
    folders["gee_folder"] = str(gee_folder)

    # Create folders   
    for folder in folders.values():
        Path(folder).mkdir(exist_ok= True)
        
    # Append db_path
    folders["db_path"] = str(db_path)

    return folders  