from ..steps import CreateSchema
from ..pipeline import SubModule
# CREATE ALL SCHEMAS FOR FUTURE TABLE AND VIEWS

create_raw_schema = CreateSchema('create_raw_schema', schema= 'raw')
create_clean_schema = CreateSchema("create_clean_schema", schema = "clean" )
create_preprocessed_schema = CreateSchema("create_preprocessed_schema", schema = "preprocessed" )
create_features_schema = CreateSchema("create_features_schema", schema = "features" )

create_all_schemas = SubModule('create_all_schemas',[create_raw_schema,
                                                create_clean_schema,
                                                create_preprocessed_schema,
                                                create_features_schema])