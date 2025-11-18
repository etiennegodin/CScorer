from jinja2 import Template
from pathlib import Path
import inspect
from pathlib import Path
from ...pipeline import Pipeline, PipelineStep, StepStatus

def who_called_me():
    # Frame 0 = this function
    # Frame 1 = direct caller
    caller_frame = inspect.stack()[2]
    return Path(caller_frame.filename)

#SQL_PATH = configs.sql_dir

def read_sql_template(file_name :str, local:bool = False):
    if local:
        parent_path = who_called_me().parent
    else:
        parent_path = Path(__file__).parent
        
    with open(parent_path / f'{file_name}.sql', 'r') as f:
        try:
            sql_template = Template(f.read())
            return sql_template

        except Exception as e:
            print(f"Error reading sql template : {e}")
        

def read_sql_file(file_name:str, local:bool = False):
    if local:
        parent_path = who_called_me().parent
    else:
        parent_path = Path(__file__).parent

    with open( parent_path/ f'{file_name}.sql', 'r') as f:
        sql_query = f.read()
    
    return sql_query
    
async def simple_sql_query(pipe:Pipeline, step:PipelineStep):
    con = pipe.con
    query = read_sql_file(step.name, local= True)
    try:
        con.execute(query)
    except Exception as e:
        pipe.logger.error(f"Failed to run query : {e}")
        step.status = StepStatus.failed
    step.status = StepStatus.completed
