from ...steps import DataBaseQuery
from ...pipeline import Module
from .observers import inat_observers_submodule
from. species import inat_species_submodule
from .ranges import inat_ranges_submodule

clean_gbif_post_inat = DataBaseQuery('clean_gbif_post_inat', query_name= 'gbif_clean_post_inat')

inat_data_module = Module('inat_data',[inat_observers_submodule,
                                       clean_gbif_post_inat,
                                       inat_species_submodule,
                                       inat_ranges_submodule], always_run= False )
