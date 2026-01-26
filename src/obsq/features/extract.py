from ..pipeline import *
from ..db import *
from .spatial_cluster import SpatialClustering 

observer_feature = SqlQuery('observers_features', "features_observers")
features_taxon_obv_id = SqlQuery('features_taxon_obv_id', "features_taxon_obv_id")
features_taxon_obv_obs = SqlQuery('features_taxon_obv_obs', "features_taxon_obv_obs")

features_identifiers = SqlQuery('features_identifiers', "features_identifiers")

community_validation_features = SqlQuery('community_validation_features', "features_community_validation")
metadata_features = SqlQuery('metadata_features', "features_metadata")
temporal_features = SqlQuery('temporal_features', "features_temporal")

features_phenology_leaves = SqlQuery('features_phenology_leaves', "features_phenology_leaves")
features_phenology_repro = SqlQuery('features_phenology_repro', "features_phenology_repro")
features_phenology_sex = SqlQuery('features_phenology_sex', "features_phenology_sex")
features_phenology_merge = SqlQuery('features_phenology_merge', "features_phenology_merge")


taxonomic_features = SqlQuery('taxonomic_features', "features_taxonomic")
histogram_features = SqlQuery('histogram_features', "features_histogram")
species_ranges_features = SqlQuery('species_ranges_features', "features_species_ranges")

spatial_clustering = SpatialClustering('spatial_clustering', type= 'kmeans', k= 6)


extract_features_module = Module('extract_features', [observer_feature,
                                                      features_taxon_obv_id,
                                                      features_taxon_obv_obs,
                                                   features_identifiers,
                                                   community_validation_features,
                                                   metadata_features,
                                                   taxonomic_features,
                                                   temporal_features,
                                                   histogram_features,
                                                   features_phenology_leaves,
                                                   features_phenology_repro,
                                                   features_phenology_sex,
                                                   features_phenology_merge,
                                                   species_ranges_features,
                                                   spatial_clustering])
