import duckdb
from pathlib import Path
import logging
import geopandas as gpd
import pandas as pd
from ..utils.core import to_Path, rename_col_df, convert_df_to_gdf
from dataclasses import dataclass, field

def _open_connection(db_path: str):
    # always create a fresh connection; use context manager where possible
    print(db_path)
    try:
        con = duckdb.connect(database=db_path)
        if load_spatial_extension(con):
            return con
        
    except Exception as e:
        logging.error(f'Error connection to duckdb {db_path} : \n ', e)
        raise IOError(f'Error connecting : {e}')

def load_spatial_extension(con):
    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        return True
    except Exception as e:
        logging.error(f"Error loading spatial extension : {e}")
        return False

async def import_csv_to_db(con :duckdb.DuckDBPyConnection, file_path:str,schema:str,table:str, replace:bool = True, geo:bool = False, delete_file: bool = False):

    if (replace) or (f"{schema}.{table}" not in get_all_tables(con)):
        create_schema(con, schema=schema)
        query = f"""CREATE OR REPLACE TABLE {schema}.{table} AS
        """
    else:
        query = f"""INSERT INTO {schema}.{table}
        """
        
    query += f"""SELECT *,
                {"ST_Point(decimalLongitude, decimalLatitude) AS geom," if geo else ""}
                FROM read_csv('{file_path}')
                """    
    try:
        con.execute(query)
        logging.info(f'Registered {file_path} to {schema}.{table}')
        return f"{schema}.{table}"

    except Exception as e:
        logging.error(f'Error creating table {schema}.{table} from file {file_path} : \n ', e)
        return None
    
async def export_to_shp(con :duckdb.DuckDBPyConnection,file_path:str,table_name:str, logger,
                        schema:str = None,
                        lon_col:str = "decimalLongitude",
                        lat_col:str = 'decimalLatitude',\
                        table_fields:list[str] = '*',
                        )-> str:
    
    if schema is not None:
        table_name = f"{schema}.{table_name}"
    
    
    if table_fields != '*':
        select = ""
        if not isinstance(table_fields, list): 
            select = f"{table_fields},"
        else:
            for field in table_fields:
                select += f"{field},"
                
        select += f"{lon_col}, {lat_col},"
    else:
        select = '*'
        
    try:
        df = con.execute(f"SELECT {select} ST_Point({lon_col}, {lat_col}) AS geom FROM {table_name}").fetch_df()
        gdf = gpd.GeoDataFrame(df.drop(columns=[lon_col, lat_col, 'geom']), geometry=gpd.points_from_xy(df[lon_col], df[lat_col]), crs=4326)
        #print(gdf)
        gdf.to_file(file_path)  
        logger.info(f"Exported shp to : {file_path}")
    except Exception as e:
        raise e
    
    return file_path    

def assign_table_alias(columns: list = None, alias :str = None):
    query = """"""
    for col in columns:
        col_new = f'{alias}.{col}'
        query += f"{col_new},\n\t\t\t\t"
    return query

def set_geom_bbox(con,table_name = None, ):

    try:
        # Add col for bbox 
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN minx DOUBLE;")
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN miny DOUBLE;")
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN maxx DOUBLE;")
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN maxy DOUBLE;")
        # Fill bbox col from geom
        con.execute(f"""UPDATE {table_name} 
                            SET minx = ST_XMin(geom),
                                miny = ST_YMin(geom),
                                maxx = ST_XMax(geom),
                                maxy = ST_YMax(geom);""")
        return True
    except Exception as e:
        logging.error(f'Could not set bbox for table {table_name} : \n {e}')
        return False
        
def create_schema(con, schema:str=None):
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        return True

    except Exception as e:
        logging.error(f'Error creating schema {schema} : \n', e)
        return False
    
def create_table(con, table:str, schema:str = 'main'):
    try:
        con.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table}")
        return True

    except Exception as e:
        logging.error(f'Error creating table {schema}.{table} : \n', e)
        return False

def get_all_tables(con)-> list[str]:
    tables = con.query("""SELECT table_schema, table_name
                FROM information_schema.tables;""").to_df().apply(lambda x: f"{x['table_schema']}.{x['table_name']}", axis = 1).to_list()
    return tables

def register_df(con, view_name:str, df:pd.DataFrame):
    try:
        con.register(view_name, df)
        return True

    except Exception as e:
        logging.error(f'Error registering {view_name} data : \n', e)
        return False

def create_table_from_view(con, view:str = None, schema:str = None, table:str = None,):
    try:
        con.execute(f"""
        CREATE OR REPLACE TABLE {schema}.{table} AS
        SELECT *,
        ST_GeomFromText(wkt) AS geom,
        FROM {view}
        """)
        return True

    except Exception as e:
        logging.error(f'Error creating table {schema}.{table} from view {view} : \n ', e)
        return False

def assign_table_alias(columns: list = None, alias :str = None):
    query = """"""
    if not isinstance(columns, list):
        raise TypeError('Error assign_table_alias, columns is not a list')
    
    for col in columns:
        col_new = f'{alias}.{col}'
        query += f"{col_new},\n\t\t\t"
    return query


    
def gdf_to_duckdb(gdf:gpd.GeoDataFrame, 
                db_path:Path,
                schema:str,
                table:str,
                view_name:str = "df_view"):
    
    if not isinstance(gdf, gpd.GeoDataFrame):
        gdf = convert_df_to_gdf(gdf)
    
    #Force Path object from db_path var
    if not isinstance(db_path, Path):
        db_path = to_Path(db_path)

    #Replace ":" with "_" in cols yo void conflict in sql query
    gdf = rename_col_df(gdf, old = ':', new = '_')

    # Convert geoemtry to wkt for duckdb spatial predicates
    gdf['wkt'] = gdf.geometry.to_wkt()
    
    #Remove geoemtry column, cant register to duckdb
    gdf = gdf.drop('geometry', axis = 1)
    
    #Commit gdf to duckdb 
    with _open_connection(db_path) as con:
        # Load spatial extensions
        load_spatial_extension(con)
        #Create schema
        create_schema(con, schema=schema)
        #Register data to view
        register_df(con, view_name = view_name, df = gdf)
        #Create table from view
        create_table_from_view(con, view = view_name, schema=schema, table=table)
        # Create bbox for spatial index use
        set_geom_bbox(con, table_name= f'{schema}.{table}')
        
        #Confirmation that table is commited to db
        confirmation_path = Path(db_path.parent / f"{schema}.{table}")
        #confirmation_path.touch()
        con.close()
        
        return f"{schema}.{table}"

def get_table_columns(table_name = None, con = None):
    columns = None
    if table_name is None:
        raise ValueError('get_table_columns - No table provided ')
    if con is None:
        raise ConnectionError("Please provide a valid duck_db connection")
    
    #Main 
    try:
        columns = con.execute(f"PRAGMA table_info('{table_name}')").df()
        return columns['name'].tolist()
    except Exception as e:
        logging.error(f'Error getting table columns : {e} ')

