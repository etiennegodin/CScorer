from ...pipeline import Module, SubModule, step, PipelineContext
from .pca import ReducerPCA
import pandas as pd
import numpy as np


### Auto load all features and 

def find_correlated(df:pd.DataFrame)->list:
    
    corr = df.corr(numeric_only=True)
    upper_tri = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    
    return [column for column in upper_tri.columns if any(upper_tri[column].abs() > 0.8)]

@step
def feature_collection_reducer(context:PipelineContext, table_name:str):

    df = context.c
    cat_col = df.select_dtypes(include='object')
    features_to_reduce = find_correlated(df)
    
    df_pca = ReducerPCA('')





obsv_pca = ReducerPCA(name = 'obsv', features_list = ['obsv_total_pct',
                                        'obsv_order_count',
                                        'obsv_family_count',
                                        'obsv_genus_count',
                                        'obsv_species_count',
                                        'obsv_unique_dates',
                                        'obsv_max_yearly_obs',
                                        'obsv_max_monthly_obs',
                                        'obsv_avg_yearly_obs',
                                        'obsv_avg_monthly_obs',
                                        'obsv_avg_descr_len',
                                        'obsv_max_species_obs_count'])

bio_pca = ReducerPCA(name = 'bio', features_list= ['bio03',
 'bio05',
 'bio06',
 'bio08',
 'bio10',
 'bio11',
 'bio12',
 'bio13',
 'bio16',
 'bio17',
 'bio18',
 'bio19'])

gee_pca = ReducerPCA(name = 'gee', features_list= ['b10',
 'b100',
 'b100_2',
 'b10_1',
 'b10_2',
 'b200',
 'b200_1',
 'b200_2',
 'b30',
 'b30_1',
 'b30_2',
 'b60',
 'b60_1',
 'b60_2'])

pca_submodule = SubModule('pca_submodule'[obsv_pca, bio_pca, gee_pca])

reduce_features_module = Module('reduce_features', [pca_submodule])