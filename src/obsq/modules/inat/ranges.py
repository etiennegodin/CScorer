import geopandas as gpd
import pandas as pd
from shapely import wkt
from pathlib import Path
import glob
from ...pipeline import *
from ...utils import gdf_to_duckdb

    
@step
def load_ranges_to_db(context:PipelineContext):
    
    base_path = Path(context.config['paths']['inat_folder'])
    polygon = context.config['gbif_loader']['GEOMETRY']
    bbox = wkt.loads(polygon)
    
    gdf = gpd.GeoDataFrame()
    count = len(glob.glob(f"{str(base_path)}/*.gpkg"))
    try:
        for i in range(1,count+1):
            print(f"Loading data from iNaturalist_geomodel_Plantae_{i}.gpkg ")
            temp_gdf = gpd.read_file(base_path/f"iNaturalist_geomodel_Plantae_{i}.gpkg", bbox=bbox)
            clipped_gdf = gpd.clip(temp_gdf, bbox)
            gdf = pd.concat([gdf, clipped_gdf], ignore_index= True)
    except Exception as e:
        raise e
    print('Registering gdf to db')
    table_name = gdf_to_duckdb(context.con, gdf, 'raw', 'species_range')
    return table_name


inat_ranges_submodule =  SubModule("inat_ranges", [load_ranges_to_db])