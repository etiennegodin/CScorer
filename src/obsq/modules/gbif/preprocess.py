from .clean import gbif_clean_submodule
from ...pipeline import *
from ...steps import *
from ..inat.observations import preprocess_inat_observations



# Join gbif_citizen to inat_observations
gbif_citizen_join_inat_obs = SimpleQuery("gbif_citizen_join_inat_obs", query_name= "gbif_citizen_join_inat_obs")

# Create observers table 
extract_all_observers = SimpleQuery("extract_all_observers", query_name= "gbif_observers_all")
#Get citizen experts 
get_citizen_expert = SimpleQuery("get_citizen_expert", query_name= "gbif_observers_expert")

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
                                           preprocess_inat_observations,
                                           gbif_citizen_join_inat_obs,
                                            filter_observers_sm

                                           ])

    