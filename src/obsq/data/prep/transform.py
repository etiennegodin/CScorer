from ...pipeline import *
from ...db import *
from .core import Transformer



features_obsv_transformed = Transformer('observer', table_id = 'recordedBy',transform_dict= { 'linear': ['obsv_unique_year_count',
                                                                                 'obsv_unique_dates',
                                                                                 'obs_per_year',
                                                                                 'obsv_avg_monthly_obs',
                                                                                 'obsv_sex_meta_pct',
                                                                                 'obsv_repro_cond_meta_pct',
                                                                                 'obsv_annot_meta_pct',
                                                                                 'obsv_avg_obs_time'],
                                                                     'log' : ['obsv_species_diversity',
                                                                              'obsv_obs_count',
                                                                              'obsv_avg_media_count',
                                                                              'obsv_avg_descr_len'
                                                                              ]
})

features_id_transformed = Transformer('identifier', table_id = 'identifiedBy',transform_dict= { 'linear': ['id_count',
                                                                                 'id_species_entropy'
                                                                                 ],
                                                                     'log' : ['obsv_avg_id_time',
                                                                              'id_avg_monthly_obs',
                                                                              'id_avg_yearly_obs',
                                                                              ]
})

features_id_transformed = Transformer('identifier', table_id = 'identifiedBy',transform_dict= { 'linear': ['id_count',
                                                                                 'id_species_entropy'
                                                                                 ],
                                                                     'log' : ['obsv_avg_id_time',
                                                                              'id_avg_monthly_obs',
                                                                              'id_avg_yearly_obs',
                                                                              ]
})

features_inat_transformed = Transformer('observer_inat', table_id = 'identifiedBy',transform_dict= { 'linear': ['inat_obv_log_z_score',
                                                                                 'inat_id_log_z_score',
                                                                                 'inat_sp_log_z_score',
                                                                                 'inat_rank_log_z_score'
                                                                                 ]
})






transform_features_module = Module('transform_features', [features_obsv_transformed, features_id_transformed, features_inat_transformed])