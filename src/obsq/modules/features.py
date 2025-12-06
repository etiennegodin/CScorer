from ..pipeline import *
from ..steps import *

observer_feature = SimpleQuery('process_observers_features', "features_observers")
community_validation_features = SimpleQuery('community_validation_features', "features_community_validation")
metadata_features = SimpleQuery('metadata_features', "features_metadata")

extractor_features = Module('extractor_features', [observer_feature, community_validation_features, metadata_features])
