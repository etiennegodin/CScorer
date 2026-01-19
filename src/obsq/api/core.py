import joblib
from ..data.ingest.inat.api import inatApiRequest



class Obsq():
    def __init__(self):
        pass
        self.model = joblib.load('model.joblib')

    def data_ingest(self):
        data = inatApiRequest('id_request','observations','id',limit=1,api_version=2)
        
        pass

    def data_prep(self):
        pass

    def score_observation(self):
        pass



