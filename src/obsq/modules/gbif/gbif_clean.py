from obsq.pipeline import PipelineContext, SubModule, FunctionStep, step
from ...utils.sql import read_sql_template
from pathlib import Path
from ...steps import CreateSchema

async def clean_gbif_occurences(context:PipelineContext, step_name:str):
    """
    Docstring for clean_gbif_occurences
    
    :param context: Description
    :type context: PipelineContext
    :param step_name: Description
    :type step_name: str
    """
    con = context.con
    sql_folder = Path(context.config['paths']['queries_folder'])
    
    table_name = step_name.split(sep='_', maxsplit=1)[-1]
    source_table_name =  f"raw.{table_name}"
    query_name = 'gbif_clean'
    file_path = sql_folder / f"{query_name}.sql"
    print(table_name)
    print(source_table_name)

    template = read_sql_template(file_path)
    query = template.render(target_table_name = table_name, source_table_name = source_table_name)
    
    try:
        con.execute(query)
        return True
    except Exception as e:
        raise


# CLEAN
@step
async def clean_gbif_citizen(context:PipelineContext):
    return clean_gbif_occurences(context, step_name="clean_gbif_citizen")
@step
async def clean_gbif_expert(context:PipelineContext):
    return clean_gbif_occurences(context, step_name="clean_gbif_expert")


gbif_clean_submodule = SubModule('gbif_clean',[clean_gbif_citizen,clean_gbif_expert ])
