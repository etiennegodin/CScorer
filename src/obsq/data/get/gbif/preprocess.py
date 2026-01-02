from ....pipeline import *
from ....db import *
from ..inat.observations import preprocess_inat_observations_submodule


clean_gbif_expert = SimpleQuery('clean_gbif_expert', query_name= 'gbif_clean_expert')
clean_gbif_citizen = SimpleQuery('clean_gbif_citizen', query_name= 'gbif_clean_citizen')

# Join gbif_citizen to inat_observations
gbif_citizen_join_inat_obs = SimpleQuery("gbif_citizen_join_inat_obs", query_name= "gbif_citizen_join_inat_obs")

# Create observers table 
extract_all_observers = SimpleQuery("extract_all_observers", query_name= "gbif_observers_all")
#Get citizen experts 
get_citizen_expert = SimpleQuery("get_citizen_expert", query_name= "gbif_observers_expert")

get_citizen_observers = SimpleQuery("get_citizen_observers", query_name= "gbif_observers_citizen")

# Send observations from citizen expert to expert table
gbif_observers_obs_shuffle = SimpleQuery("gbif_observers_obs_shuffle", query_name= "gbif_observers_obs_shuffle" )

gbif_preprocess_observers_submodule = SubModule("gbif_preprocess_observers",[extract_all_observers,
                                                get_citizen_expert,
                                                get_citizen_observers,
                                                gbif_observers_obs_shuffle])

# FULL MODULE 
preprocess_gbif_module = Module("preprocess_gbif", [
                                           clean_gbif_expert,
                                           clean_gbif_citizen,
                                           preprocess_inat_observations_submodule,
                                           gbif_citizen_join_inat_obs,
                                            gbif_preprocess_observers_submodule
                                           ])

    