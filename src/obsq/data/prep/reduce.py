from ...pipeline import Module, SubModule, step, PipelineContext
from .reducer import Reducer
import pandas as pd
import numpy as np


### Auto load all features and 

obsv_reducer = Reducer(name = 'observer', table_id='recordedBy' )
spatial_reducer = Reducer(name = 'spatial')
community_validation_reducer = Reducer(name = 'community_validation')
histogram_reducer = Reducer(name = 'histogram')
metadata_reducer = Reducer(name = 'metadata')
phenology_leaves_reducer = Reducer(name = 'phenology_leaves')
phenology_repro_reducer = Reducer(name = 'phenology_repro')
phenology_sex_reducer = Reducer(name = 'phenology_sex')
taxonomic_reducer = Reducer(name = 'taxonomic', table_id='taxonID' )
temporal_repro_reducer = Reducer(name = 'temporal')
gee_reducer = Reducer(name = 'gee')


reduce_features_module = Module('reduce_features', [obsv_reducer,
                                                    spatial_reducer,
                                                    community_validation_reducer,
                                                    histogram_reducer,
                                                    metadata_reducer,
                                                    phenology_leaves_reducer,
                                                    phenology_repro_reducer,
                                                    phenology_sex_reducer,
                                                    taxonomic_reducer,
                                                    temporal_repro_reducer,
                                                    gee_reducer])