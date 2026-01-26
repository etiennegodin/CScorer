from ....pipeline import PipelineContext, step, Module
from .core import GeeContext, GeePointSampler, wkt_string_to_geojson, init_gee
import ee
from pprint import pprint

@step
async def gee_features_citizen(context:PipelineContext):
    init_gee()
    config = context.config
    aoi_wkt = wkt_string_to_geojson(config['gbif_loader']['GEOMETRY'])
    aoi = ee.Geometry.Polygon(aoi_wkt['coordinates'])
    points = ee.FeatureCollection('projects/observationscorer/assets/citizen_occurences')
    gee_context = GeeContext(aoi, points, config['time']['start'], config['time']['end'])         

    sampler = GeePointSampler('citizen_occurences', gee_context)
    return sampler._execute(context)


@step
async def gee_features_expert(context:PipelineContext):
    init_gee()
    config = context.config
    aoi_wkt = wkt_string_to_geojson(config['gbif_loader']['GEOMETRY'])
    aoi = ee.Geometry.Polygon(aoi_wkt['coordinates'])
    points = ee.FeatureCollection('projects/observationscorer/assets/expert_occurences')
    gee_context = GeeContext(aoi, points, config['time']['start'], config['time']['end'])         
       
    sampler = GeePointSampler('expert_occurences', gee_context)
    return sampler._execute(context)

gee_features_module = Module('gee_features',[gee_features_expert, gee_features_citizen])



