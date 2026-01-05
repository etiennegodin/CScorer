from ...pipeline import *
from ...db import *
from .spatial_cluster import SpatialClustering 

observer_feature = SimpleQuery('observers_features', "features_observers")
community_validation_features = SimpleQuery('community_validation_features', "features_community_validation")
metadata_features = SimpleQuery('metadata_features', "features_metadata")
temporal_features = SimpleQuery('temporal_features', "features_temporal")

features_phenology_leaves = SimpleQuery('features_phenology_leaves', "features_phenology_leaves")
features_phenology_repro = SimpleQuery('features_phenology_repro', "features_phenology_repro")
features_phenology_sex = SimpleQuery('features_phenology_sex', "features_phenology_sex")

taxonomic_features = SimpleQuery('taxonomic_features', "features_taxonomic")
histogram_features = SimpleQuery('histogram_features', "features_histogram")
combined_features = SimpleQuery('combined_features', "features_combined")

spatial_clustering = SpatialClustering('spatial_clustering', max_dist= 5)

extractor_features_module = Module('extractor_features', [observer_feature,
                                                   community_validation_features,
                                                   metadata_features,
                                                   taxonomic_features,
                                                   temporal_features,
                                                   histogram_features,
                                                   features_phenology_leaves,
                                                   features_phenology_repro,
                                                   features_phenology_sex,
                                                   spatial_clustering,
                                                   combined_features])
