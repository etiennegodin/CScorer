from ..utils.duckdb import _open_connection
from ..utils import to_Path
from ..pipeline import ClassStep
import duckdb

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
       


async def import_csv_to_db(con :duckdb.DuckDBPyConnection,
                           file_path:str,
                           schema:str,
                           table:str,
                           replace:bool = True,
                           geo:bool = False,
                           delete_file: bool = False)->str:
    """
    Docstring for import_csv_to_db
    
    :param con: Description
    :type con: duckdb.DuckDBPyConnection
    :param file_path: Description
    :type file_path: str
    :param schema: Description
    :type schema: str
    :param table: Description
    :type table: str
    :param replace: Description
    :type replace: bool
    :param geo: Description
    :type geo: bool
    :param delete_file: Description
    :type delete_file: bool
    :return: Description
    :rtype: str
    """
    logger = logging.getLogger("import_csv_to_db")
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
        logger.info(f'Registered {file_path} to {schema}.{table}')
        return f"{schema}.{table}"

    except Exception as e:
        logger.error(f'Error creating table {schema}.{table} from file {file_path} : \n ', e)
        return None     