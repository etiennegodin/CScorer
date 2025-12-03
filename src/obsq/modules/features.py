from obsq.pipeline import PipelineContext, Module, SubModule, step
from pathlib import Path
from ..steps import DataBaseQuery

observer_feature = DataBaseQuery('process_observers_features', "features_observers")




all_features = Module('extractor_features', [observer_feature])