from ....pipeline import *
from ....steps import SimpleQuery
from ....utils.sql import read_sql_template


clean_gbif_expert = SimpleQuery('clean_gbif_expert', query_name= 'gbif_clean_expert')
clean_gbif_citizen = SimpleQuery('clean_gbif_citizen', query_name= 'gbif_clean_citizen')

gbif_clean_submodule = SubModule('gbif_clean',[clean_gbif_citizen,clean_gbif_expert ])
