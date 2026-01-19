from ...pipeline import * 
from ...db import *

# Set target variable 
# Label citizen obs 

label_citizen_data = SqlQuery('label_citizen_data')

score_spatial = SqlQuery('score_spatial')
score_identifier = SqlQuery('score_identifier')
score_observer = SqlQuery('score_observer')
score_phenology = SqlQuery('score_phenology')


score_total = SqlQuery('score_sum')


score_obs_module = Module('score_observations',[score_spatial, score_identifier, score_observer, score_phenology, score_total])

#score_obs_module = Module('score_observations',[score_spatial, score_expert_validation, score_community, score_data_quality, score_scientific_value, score_combined])
