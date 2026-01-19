import joblib
from ..data.ingest.inat.api import inatObservationApi
import asyncio
import pandas as pd 
from typing import Union

class Obsq():
    def __init__(self):
        pass
        #self.model = joblib.load('model.joblib')
        

    def get_observations_data(self, ids):
        return asyncio.run(inatObservationApi('id_request', ids).request())

    def data_clean(self):
        pass
    def data_prep(self):
        pass

    def score_observation(self, ids:Union[str,list]):
        observations_data = self.get_observations_data(ids)
        user_data = observations_data['user']['id']





        pass



