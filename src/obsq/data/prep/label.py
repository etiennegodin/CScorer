from ...pipeline import * 
from ...db import *

# Set target variable 
# Label citizen obs 

label_citizen_data = SimpleQuery('label_citizen_data')


spatial_match = SimpleQuery('score_spatial')
expert_validation = SimpleQuery('')
community_consensus = SimpleQuery('')
phenology_match = SimpleQuery('')

label_data_module = Module('label_data',[label_citizen_data, spatial_match])