from ...pipeline import *
from ...steps import TemplateQuery
from ...utils.sql import read_sql_template


clean_gbif_expert = TemplateQuery('clean_gbif_expert', fields = {"target_table_name": "gbif_expert", "source_table_name" : "raw.gbif_expert"  }, query_name= 'gbif_clean')
clean_gbif_citizen = TemplateQuery('clean_gbif_citizen', fields = {"target_table_name": "gbif_citizen", "source_table_name" : "raw.gbif_citizen"  }, query_name= 'gbif_clean')

gbif_clean_submodule = SubModule('gbif_clean',[clean_gbif_citizen,clean_gbif_expert ])
