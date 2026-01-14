from ...pipeline import * 
from ...db import *

# Set target variable 
# Label citizen obs 

label_citizen_data = SimpleQuery('label_citizen_data')


spatial_match = SimpleQuery('score_spatial')
expert_validation = SimpleQuery('score_expert_validation')
score_total = SimpleQuery('score_sum')

score_obsv_module = Module('score_observations',[spatial_match, expert_validation, score_total])