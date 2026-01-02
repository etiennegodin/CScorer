from ...pipeline import * 
from ...db import *

# Set target variable 
# Label citizen obs 

label_citizen_data = SimpleQuery('label_citizen_data')

label_data_module = Module('label_data',[label_citizen_data])