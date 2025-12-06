from ..pipeline import *
from ..steps import *

observer_feature = SimpleQuery('process_observers_features', "features_observers")
community_validation_features = SimpleQuery('community_validation_features', "features_community_validation")
metadata_features = SimpleQuery('metadata_features', "features_metadata")
phenology_features = SimpleQuery('phenology_features', "features_phenology")
taxonomic_features = SimpleQuery('taxonomic_features', "features_taxonomic")
histogram_features = SimpleQuery('histogram_features', "features_histogram")
combined_features = SimpleQuery('combined_features', "features_combined")


extractor_features = Module('extractor_features', [observer_feature,
                                                   community_validation_features,
                                                   metadata_features,
                                                   phenology_features,
                                                   taxonomic_features,
                                                   histogram_features,
                                                   combined_features])
