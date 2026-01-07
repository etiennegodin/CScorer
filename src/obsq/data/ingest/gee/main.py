from ....pipeline import PipelineContext, step
from .core import GeeContext, GeePointSampler, wkt_string_to_geojson
import ee

@step
def gee_features_citizen(context:PipelineContext):
    config = context.config
    aoi_wkt = wkt_string_to_geojson(config['gbif_loader']['GEOMETRY'])
    aoi = ee.Geometry.Polygon(aoi)
    points = ee.FeatureCollection('projects/observationscorer/assets/citizen_occurences')
    context = GeeContext(aoi, points, config['time']['start'], config['time']['end']
                         


    )

