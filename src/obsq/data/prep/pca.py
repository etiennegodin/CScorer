from ...pipeline import PipelineContext, ClassStep
from sklearn.decomposition import PCA
import numpy as np
import pandas as pd


class Reducer(ClassStep):
    def __init__(self, name, features_list:list,variance_threshold = 0.98,**kwargs):
        """
        Pca wrapper
        """
        super().__init__(name, retry_attempts =1,**kwargs)

        self.name = name
        self.table_name = f"features.pca_{name}"
        self.variance_threshold = variance_threshold

        if not isinstance(features_list,list):
            raise TypeError("Provided features for pca is not a list")
        self.features_list = features_list

    def _execute(self, context):
        df = context.con.execute("SELECT * FROM features.combined").df()
        
        df_pca = self._pca_reducer(df)

        return super()._execute(context)
        

    def _find_correlated(self, df:pd.DataFrame)->list:
        
        corr = df.corr(numeric_only=True)
        upper_tri = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
        
        return [column for column in upper_tri.columns if any(upper_tri[column].abs() > 0.8)]
    def _pca_reducer(self, df):
        variance = 0.0
        n_components = 1
        self.retry_wait_maxcat_col = df.select_dtypes(include='object')

        ids = df['gbifID']
        df_pruned = df[self.features_list]

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

        self.logger.info(f'Ran pca on {self.name} | {n_components} components explained {round(variance,3)}% of variance')
        return df_pca
    
    def _create_rename_dict(self, n_components)->dict:
        columns_rename = {}
        for i in range(n_components):
            columns_rename[i] = f"{self.name}_pca{i}"
        return columns_rename
    

        


# Core for pca 
