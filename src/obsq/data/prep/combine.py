from ...pipeline import *
from ...db import *
from .core import Combine





combined_features = SqlQuery('combined_features', "features_combined")

combined_features_p = Combine('combined_features_p', combine_dict = {'observer' : ('recordedBy', 'left')

        
    
                })

combine_features_module = Module('combine_features', [combined_features, combined_features_p])


x = {
                    'observer_inat' : "recordedBy",
                'taxonomic' : 'taxonID',
                'community_validation' : 'gbifID',
                'metadata' : 'gbifID',
                'histogram' : 'gbifID',
                'temporal' : 'gbifID',
                'spatial':'gbifID',
                'gee' :'gbifID',
                'ranges' : 'gbifID'
}