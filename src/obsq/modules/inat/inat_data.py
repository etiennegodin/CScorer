from ...steps import DataBaseQuery
from ...pipeline import Module, SubModule
from .observers import inat_observers_submodule
from. species import inat_species_submodule
from .ranges import inat_ranges_submodule

clean_gbif_post_inat = DataBaseQuery('clean_gbif_post_inat', query_name= 'gbif_clean_post_inat')
create_species_table = DataBaseQuery('create_species_table', query_name= 'gbif_extract_species')

gbif_post_inat_observers = SubModule("gbif_post_inat_observers", [clean_gbif_post_inat, create_species_table])


inat_data_module = Module('inat_data',[inat_observers_submodule,
                                       gbif_post_inat_observers,
                                       inat_species_submodule,
                                       inat_ranges_submodule], always_run= False )

