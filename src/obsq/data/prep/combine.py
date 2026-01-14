from ...pipeline import *
from ...db import *

combined_features = SqlQuery('combined_features', "features_combined")

combine_features_module = Module('combine_features', [combined_features])