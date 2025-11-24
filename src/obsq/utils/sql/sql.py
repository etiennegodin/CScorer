from jinja2 import Template
from pathlib import Path
import inspect
from pathlib import Path
from ...pipeline import Pipeline, PipelineStep, StepStatus
import asyncio
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
    
async def simple_sql_query(pipe:Pipeline, step:PipelineStep, sql_folder:Path):
    con = pipe.con
    file_name = step.name
    file_path = sql_folder / f"{file_name}.sql"
    query = read_sql_file(file_path)
    
    try:
        con.execute(query)

    except Exception as e:
        pipe.logger.error(f"Failed to run query :\n{e}")
        step.status = StepStatus.failed
        raise RuntimeError(e)
    
    step.status = StepStatus.completed

    
