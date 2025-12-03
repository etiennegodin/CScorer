import geopandas as gpd
import pandas as pd
from shapely import wkt
from pathlib import Path

from ...pipeline import step, SubModule, PipelineContext
from ...utils import gdf_to_duckdb




    
@step
def load_ranges_to_db(context:PipelineContext):
    
    base_path = Path(context.config['paths']['inat_folder'])
    polygon = context.config['gbif_loader']['GEOMETRY']
    bbox = wkt.loads(polygon)
    
    gdf = gpd.GeoDataFrame()

    try:
        for i in range(1,10):
            temp_gdf = gpd.read_file(base_path/f"iNaturalist_geomodel_Plantae_{i}.gpkg", bbox=bbox)
            gdf = pd.concat([gdf, temp_gdf], ignore_index= True)
    except Exception as e:
        raise e
    
    table_name = gdf_to_duckdb(context.con, gdf, 'raw', 'species_range')
    
    return table_name


inat_ranges_submodule =  SubModule("inat_ranges", [load_ranges_to_db])