from ...pipeline import Module, SubModule, step, PipelineContext
from .reducer import Reducer
import pandas as pd
import numpy as np


### Auto load all features and 

obsv_reducer = Reducer(name = 'observer', table_id='recordedBy' )

reduce_features_module = Module('reduce_features', [obsv_reducer])