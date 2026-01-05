import numpy as np
from sklearn.cluster import DBSCAN
from ...pipeline import ClassStep, PipelineContext
 
class SpatialClustering(ClassStep):
    def __init__(self, name,
                max_dist = 50,
                min_samples =2,
                **kwargs):

        self.kms_per_radian = 6371. # Earth's radius in kilometers
        self.max_dist = max_dist # Define the maximum distance for a cluster in kilometers
        self.eps_radians = self.max_dist / self.kms_per_radian
        self.min_samples = min_samples
        self.table_name = "features.spatial"
        super().__init__(name, **kwargs)
    
    def _execute(self, context:PipelineContext):
        con = context.con
        df = con.execute("SELECT gbifID, decimalLatitude, decimalLongitude FROM labeled.gbif_citizen" ).df()
        coords = df[['decimalLatitude', 'decimalLongitude']].to_numpy()
        coords_radians = np.radians(coords)
        db = DBSCAN(eps=self.eps_radians, min_samples=self.min_samples, algorithm='ball_tree', metric='haversine')
        cluster_labels = db.fit_predict(coords_radians)
        df['spatial_cluster'] = cluster_labels
        self.logger.info(f"Created {df['spatial_cluster'].nunique()} clusters using max distance of {self.max_dist}km")

        con.execute(f"CREATE OR REPLACE TABLE {self.table_name} AS SELECT * FROM df" )



