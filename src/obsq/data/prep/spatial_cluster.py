import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN, KMeans
from ...pipeline import ClassStep, PipelineContext
from typing import Literal

_cluster_algo = Literal["dbscan", 'kmeans' ]

class SpatialClustering(ClassStep):
    def __init__(self, name,
                type: _cluster_algo = "kmeans",
                k:int = 5,
                max_dist:float = 50,
                **kwargs):

        self.type = type
        if self.type == 'dbscan':    
            kms_per_radian = 6371. # Earth's radius in kilometers
            self.max_dist = max_dist # Define the maximum distance for a cluster in kilometers
            self.hyper_param = self.max_dist / kms_per_radian
        elif self.type == 'kmeans':
            self.hyper_param = k

        self.table_name = "features.spatial"

        super().__init__(name, **kwargs)
    
    def _execute(self, context:PipelineContext):
        con = context.con
        df_init = con.execute("SELECT gbifID, decimalLatitude, decimalLongitude FROM preprocessed.gbif_citizen" ).df()

        if self.type == 'dbscan':
            df = self._db_scan(df_init)
        elif self.type == 'kmeans':
            df = self._kmeans(df_init)
        
        con.execute(f"CREATE OR REPLACE TABLE {self.table_name} AS SELECT * FROM df" )

    def _db_scan(self,df:pd.DataFrame)->pd.DataFrame:
        coords = df[['decimalLatitude', 'decimalLongitude']].to_numpy()
        coords_radians = np.radians(coords)
        db = DBSCAN(eps=self.hyper_param, min_samples=2, algorithm='ball_tree', metric='haversine')
        cluster_labels = db.fit_predict(coords_radians)
        df['spatial_cluster'] = cluster_labels
        self.logger.info(f"Created {df['spatial_cluster'].nunique()} clusters using max distance of {self.max_dist}km")
        return df
    
    def _kmeans(self,df:pd.DataFrame)->pd.DataFrame:
        coords = df[['decimalLatitude', 'decimalLongitude']]
        kmeans_model = KMeans(n_clusters=self.hyper_param, init='k-means++', random_state=42)
        y_kmeans = kmeans_model.fit_predict(coords) # Predict the cluster for each point
        df['spatial_cluster'] = y_kmeans
        self.logger.info(f"Created {df['spatial_cluster'].nunique()} clusters using k of {self.hyper_param}")
        return df
    
    def _elbow_k(df):
        wcss = []
        for i in range(1, 11):
            kmeans = KMeans(n_clusters=i, init='k-means++', random_state=42)
            kmeans.fit(df)
            wcss.append(kmeans.inertia_)

