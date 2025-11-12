from jinja2 import Template
from pathlib import Path

#SQL_PATH = configs.sql_dir

def read_sql_template(file_name : str = None):
    with open(Path(__file__).parent / f'{file_name}.sql', 'r') as f:
        try:
            sql_template = Template(f.read())
            return sql_template

        except Exception as e:
            print(f"Error reading sql template : {e}")
        

def read_sql_file(file_name):
    with open(Path(__file__).parent / f'{file_name}.sql', 'r') as f:
        sql_query = f.read()
    
    return sql_query
    
