from ..pipeline import Module
from .pca import runPCA


obsv_pca = runPCA(name = 'obsv', features_list = ['obsv_total_pct',
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

bio_pca = runPCA(name = 'bio', features_list= ['bio03',
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

gee_pca = runPCA(name = 'gee', features_list= ['b10',
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

pca_module = Module('pca_module', [obsv_pca, bio_pca, gee_pca])