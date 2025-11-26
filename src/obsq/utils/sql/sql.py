from jinja2 import Template
from pathlib import Path
import inspect
from pathlib import Path
from ...pipeline import PipelineContext, StepStatus
from ..core import to_Path

def who_called_me():
    # Frame 0 = this function
    # Frame 1 = direct caller
    caller_frame = inspect.stack()[2]
    return Path(caller_frame.filename)

#SQL_PATH = configs.sql_dir

def read_sql_template(file_path:Path):

    with open(file_path, 'r') as f:
        try:
            sql_template = Template(f.read())
            return sql_template

        except Exception as e:
            print(f"Error reading sql template : {e}")
        

def read_sql_file(file_path:Path):
    with open(file_path, 'r') as f:
        sql_query = f.read()
    
    return sql_query
    
async def simple_sql_query(context:PipelineContext, query_file:str, sql_folder:Path):
    con = context.con
    query_path = to_Path(query_file)
    if query_path.suffix != "sql":
        query_path = query_path.parent / f"{query_path.stem}.sql"
        
    file_path = sql_folder / query_path
    query = read_sql_file(file_path)
    
    try:
        con.execute(query)
        return True
        
    except Exception as e:
        raise e
    

    
