from ..utils.duckdb import _open_connection
from ..utils import to_Path
from ..pipeline import ClassStep

class CreateSchema(ClassStep):

    def __init__(self, name, schema:str = "main", **kwargs):
        """_summary_

        Args:
            name (_type_): _description_
            schema (str, optional): _description_. Defaults to "main".
        """
        super().__init__(name, **kwargs)
        self.name = name
        self.schema = schema
    
    def _execute(self, context):
        con = _open_connection(context.get_step_output("db_connection"))
        
        if self.schema is None:
            raise ValueError(f'Not schema provided for {self.name} ')
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            return self.schema

        except Exception as e:
            self.logger.error(f'Error creating schema {self.schema} : \n', e)
            return None
            