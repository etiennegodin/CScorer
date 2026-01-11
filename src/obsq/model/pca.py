from ..pipeline import PipelineContext, ClassStep
import pandas as pd
from sklearn.decomposition import PCA


class runPCA(ClassStep):
    def __init__(self, name,
                  features_list:list,
                  variance_threshold = 0.98,**kwargs):
        
        super().__init__(name, retry_attempts =1,**kwargs)

        self.name = name
        self.table_name = f"features.pca_{name}"
        self.variance_threshold = variance_threshold

        if not isinstance(features_list,list):
            raise TypeError("Provided features for pca is not a list")
        self.features_list = features_list

    def _execute(self, context):
        variance = 0.0
        n_components = 1
        df_raw = context.con.execute("SELECT * FROM features.combined").df()
        ids = df_raw['gbifID']
        df_pruned = df_raw[self.features_list]

        #Iterate until variance explained
        for i in range(len(self.features_list)):
            if variance > self.variance_threshold:
                break
            n_components += 1
            pca = PCA(n_components=n_components)
            pca.fit(df_pruned)
            variance = sum(pca.explained_variance_ratio_)
            
        #Transform original data 
        df_pca = pd.DataFrame(pca.transform(df_pruned))

        #Rename
        columns_rename = self._create_rename_dict(n_components)
        df_pca = df_pca.rename(columns=columns_rename)

        #Add back ids
        df_pca['gbifID'] = ids

        #Export
        context.con.execute(f"CREATE OR REPLACE TABLE {self.table_name} AS SELECT * FROM df_pca")
        self.logger.info(f'Ran pca on {self.name} | {n_components} components explained {round(variance,3)}% of variance')
        return super()._execute(context)
    
    def _create_rename_dict(self, n_components)->dict:
        columns_rename = {}
        for i in range(n_components):
            columns_rename[i] = f"{self.name}_pca{i}"
        return columns_rename
    

        


# Core for pca 
