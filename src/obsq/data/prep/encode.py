from ...pipeline import Module, SubModule, step, PipelineContext
from .encoder import Encoder
import pandas as pd
import numpy as np


### Auto load all features and 

obsv_reducer = Encoder(name = 'observer', table_id='recordedBy' )
spatial_reducer = Encoder(name = 'spatial')
community_validation_reducer = Encoder(name = 'community_validation')
histogram_reducer = Encoder(name = 'histogram')
metadata_reducer = Encoder(name = 'metadata')
phenology_leaves_reducer = Encoder(name = 'phenology_leaves')
phenology_repro_reducer = Encoder(name = 'phenology_repro')
phenology_sex_reducer = Encoder(name = 'phenology_sex')
taxonomic_reducer = Encoder(name = 'taxonomic', table_id='taxonID' )
temporal_repro_reducer = Encoder(name = 'temporal')
gee_reducer = Encoder(name = 'gee')


encode_features_module = Module('encode_features', [obsv_reducer,
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