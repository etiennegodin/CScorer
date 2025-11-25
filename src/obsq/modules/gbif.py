from obsq.pipeline import Module
from ..steps import GbifLoader


collect_citizen_data = GbifLoader('collect_citizen_data', predicates = {'BASIS_OF_RECORD': 'HUMAN_OBSERVATION'})

m_collect_gbif = Module("collect_gbif",[collect_citizen_data])
