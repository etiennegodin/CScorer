from ..pipeline import *
from ..steps import *

observer_feature = SimpleQuery('process_observers_features', "features_observers")
extractor_features = Module('extractor_features', [observer_feature])
