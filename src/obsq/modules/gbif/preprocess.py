from .clean import gbif_clean_submodule
from ...pipeline import *
from ...steps import *

# Create observers table 
extract_all_observers = SimpleQuery("extract_all_observers", query_name= "gbif_extract_all_observers")
#Get citizen experts 
get_citizen_expert = SimpleQuery("get_citizen_expert", query_name= "gbif_get_citizen_expert")

get_citizen_observers = SimpleQuery("get_citizen_observers", query_name= "gbif_get_citizen_observers")

# Send observations from citizen expert to expert table
citizen_occ_to_expert = SimpleQuery("citizen_occ_to_expert", query_name= "gbif_citizen_occ_to_expert" )

filter_observers_sm = SubModule("filter_observers",[extract_all_observers,
                                                get_citizen_expert,
                                                get_citizen_observers,
                                                citizen_occ_to_expert])

# FULL MODULE 
gbif_preprocess = Module("preprocess_gbif", [
                                           gbif_clean_submodule,
                                            filter_observers_sm

                                           ])

    