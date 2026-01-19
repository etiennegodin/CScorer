from ...pipeline import * 
from ...db import *

# Set target variable 
# Label citizen obs 

label_citizen_data = SqlQuery('label_citizen_data')

score_spatial = SqlQuery('score_spatial')
score_expert_validation = SqlQuery('score_expert_validation')
score_community = SqlQuery('score_community')
score_data_quality = SqlQuery('score_data_quality')
score_scientific_value = SqlQuery('score_scientific_value')
score_combined = SqlQuery('score_combined')

score_total = SqlQuery('score_sum')

score_class = SqlQuery('score_class')

score_obs_module = Module('score_observations',[score_spatial])

#score_obs_module = Module('score_observations',[score_spatial, score_expert_validation, score_community, score_data_quality, score_scientific_value, score_combined])
