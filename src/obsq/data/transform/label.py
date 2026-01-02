from ...pipeline import * 
from ...steps import SimpleQuery

# Set target variable 
# Label citizen obs 

label_citizen_data = SimpleQuery('label_citizen_data')

label_data = Module('label_data',[label_citizen_data])