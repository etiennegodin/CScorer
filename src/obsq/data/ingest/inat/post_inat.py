from ....db import *
from ....pipeline import *

clean_gbif_post_inat = SimpleQuery('clean_gbif_post_inat', query_name= 'gbif_clean_post_inat')
gbif_post_inat_observers_module = Module("gbif_post_inat_observers", [clean_gbif_post_inat])

