from ..pipeline import *
from ..db import *
from .core import Combine





combined_features = SqlQuery('combined_features', "features_combined")

combined_features_p = Combine('combined_features_p', combine_dict = [('observer','recordedBy', 'inner'),
                                                                     ('observer_inat','recordedBy','inner'),
                                                                     ('taxonomic', 'taxonID','inner'),
                                                                     ('community_validation', 'gbifID','inner'),
                                                                     ('metadata','gbifID','inner'),
                                                                     ('histogram','gbifID','inner'),
                                                                     ('temporal','gbifID','inner'),
                                                                     ('spatial','gbifID','inner'),
                                                                     ('gee','gbifID','inner'),
                                                                     ('ranges','gbifID','inner'),
                                                                     ('phenology', 'gbifID','left')])
                                                    
        
    


combine_features_module = Module('combine_features', [combined_features, combined_features_p])

