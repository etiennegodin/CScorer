from .observers import inat_observers_submodule
from .species import inat_species_submodule
from .ranges import inat_ranges_submodule
from ....db import *
from ....pipeline import *

clean_gbif_post_inat = SimpleQuery('clean_gbif_post_inat', query_name= 'gbif_clean_post_inat')

gbif_post_inat_observers = SubModule("gbif_post_inat_observers", [clean_gbif_post_inat])


inat_data = Module('inat_data',[inat_observers_submodule,
                                    clean_gbif_post_inat,
                                       inat_species_submodule,
                                       inat_ranges_submodule], always_run= False )


