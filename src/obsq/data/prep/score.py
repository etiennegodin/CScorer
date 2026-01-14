from ...pipeline import * 
from ...db import *

# Set target variable 
# Label citizen obs 

label_citizen_data = SqlQuery('label_citizen_data')


spatial_match = SqlQuery('score_spatial')
expert_validation = SqlQuery('score_expert_validation')
score_total = SqlQuery('score_sum')

score_class = SqlQuery('score_class')


score_obs_module = Module('score_observations',[spatial_match, expert_validation, score_total])
