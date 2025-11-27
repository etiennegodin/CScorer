from ..utils.duckdb import _open_connection
from ..utils import to_Path
from ..pipeline import ClassStep



class DataBaseConnection(ClassStep):
    
    def __init__(self, db_path, **kwargs):
        """_summary_

        Args:
            db_path (_type_): _description_
        """
        self.name = "db_connection"
        super().__init__(self.name, **kwargs)
        self.db_path = db_path
        self.con = _open_connection(db_path)
    
    def _execute(self, context):
        context.con = self.con
        print(f"Initiliazing database connection: {self.db_path}")
        return self.db_path

class DataBaseQuery(ClassStep):

    def __init__(self, name, query_name:str = None, **kwargs):
        """_summary_

        Args:
            name (_type_): _description_
            query_name (str, optional): _description_. Defaults to None.
        """
        super().__init__(name, **kwargs)
        
        if query_name is not None:
            self.query_file_name = query_name
        self.query_file_name = name
    
    def _execute(self, context):
        con = _open_connection(context.get_step_output("db_connection"))
        
        query = self._get_query(context)
        
        try:
            con.execute(self.query)
        except Exception as e: 
            self.logger.error(e)
            
    def _get_query(self, context):
        query_path = to_Path(self.query_file_name) #trick to get .suffix and .stem
        if query_path.suffix != "sql":
            query_path = query_path.parent / f"{query_path.stem}.sql"
            
        file_path = sql_folder / query_path
            
class DataBaseDfLoader(ClassStep):
    
    def __init__(self, name, query:str, **kwargs):
        super().__init__(name, **kwargs)
        self.query = query
    
    def _execute(self, context):
        con = context.get_step_output("db_connection")
        try:
            df = con.execute(self.query).df()
        except Exception as e: 
            self.logger.error(e)
            
        return df 
        


            
        


        
        
    
    


