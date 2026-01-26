import joblib
from .inat import inatObservationApi
from ..pipeline import * 
import asyncio
import pandas as pd 
from typing import Union
from pprint import pprint


from ..utils import *
from ..pipeline import *
from ..db import * 
from pathlib import Path
from ..data.ingest.gee.main import gee_features_module


class ObservationScorer():
    def __init__(self):
        pass
        #self.model = joblib.load('model.joblib')

    def data_ingest_observation_data(self, ids):
        return asyncio.run(inatObservationApi('id', ids=ids).request())
    
    def data_ingest_user_observations(self, user_id):
        return asyncio.run(inatObservationApi('user_id', ids=user_id).request())
    
    def data_ingest_user_data(self, user_id):
        return asyncio.run(inatObservationApi('user_id', ids=user_id).request())

    def data_ingest_user_data(self, user_id):
        return asyncio.run(inatObservationApi('user_id', ids=user_id).request())


    def data_clean(self):
        pass
    def data_prep(self):
        pass

    def score_observation(self, ids:Union[str,list]):

        observations = self.data_ingest_observation_data(ids)
        if len(observations) < 1:
            raise UserWarning('No observations')
        pprint(observations[0])
        """
        for observation in observations:
            user_observations = self.data_ingest_user_observations(observation['user']['id'])
            user_data = 'x'
            user_observations_species_data = 'x'
        """

    def _observation_formatter(self,data:dict)->pd.DataFrame:
        pass



