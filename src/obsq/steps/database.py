from ..utils.duckdb import _open_connection
from ..pipeline import ClassStep

class DataBaseConnection(ClassStep):
    
    def __init__(self, db_path, **kwargs):
        self.name = "db_connection"
        super().__init__(self.name, **kwargs)
        self.db_path = db_path
        self.con = _open_connection(db_path)
    
    def _execute(self, context):
        context.con = self.con
        print(f"Initiliazing database connection: {self.db_path}")
        return self.db_path

class DataBaseQuery(ClassStep):
    
    def __init__(self, name, query:str, **kwargs):
        super().__init__(name, **kwargs)
        self.query = query
    
    def _execute(self, context):
        con = _open_connection(context.get_step_output("db_connection"))
        try:
            con.execute(self.query)
        except Exception as e: 
            self.logger.error(e)
            
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
        


            
        


        
        
    
    


