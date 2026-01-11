from ...pipeline import PipelineContext, ClassStep
from sklearn.decomposition import PCA
import numpy as np
import pandas as pd


class Reducer(ClassStep):
    def __init__(self, name, table_id:str = 'gbifID', variance_threshold = 0.98,**kwargs):
        """
        Pca wrapper
        """
        self.features_name = name
        self.table_id = table_id
        self.input_table_name = f"features.{name}"
        self.output_table_name = f"features.pca_{name}"
        self.variance_threshold = variance_threshold
        super().__init__(name, retry_attempts =1,**kwargs)


    def _execute(self, context):
        #Load df
        df = context.con.execute(f"SELECT * FROM {self.input_table_name}").df()

        ids = df[self.table_id]
        #Split columns, keep non-numeric for later
        df_out = df.select_dtypes(include='object')
        # Find highligh correlated features 
        to_reduce = self._find_correlated(df)
        if len(to_reduce) == 0:
            return 
        #Reduce with pca
        df_pca = self._pca_reducer(df, to_reduce)

        #Set output
        if df_out.empty != True:
            df_out[self.table_id] = ids
            df_out = pd.merge(df_out, df_pca, on = self.table_id)
        else:
            df_out = df_pca

        context.con.execute(f"CREATE OR REPLACE TABLE {self.output_table_name} AS SELECT * FROM df_out")

        return super()._execute(context)
        

    def _find_correlated(self, df:pd.DataFrame)->list:
        df = df.drop(columns=[self.table_id])
        corr = df.corr(numeric_only=True)
        upper_tri = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
        return [column for column in upper_tri.columns if any(upper_tri[column].abs() > 0.8)]
    
    def _pca_reducer(self, df, features_list:list)->pd.DataFrame:
        variance = 0.0
        n_components = 1
        self.retry_wait_maxcat_col = df.select_dtypes(include='object')

        ids = df[self.table_id]
        df_pruned = df[features_list]

        #Iterate until variance explained
        for i in range(len(features_list)):
            if variance > self.variance_threshold:
                break
            n_components += 1
            pca = PCA(n_components=n_components)
            pca.fit(df_pruned)
            variance = sum(pca.explained_variance_ratio_)

        self.logger.info(f'Ran pca on {self.features_name} | {n_components} components explained {round(variance,3)}% of variance')

        #Transform original data 
        df_pca = pd.DataFrame(pca.transform(df_pruned))

        #Rename
        columns_rename = self._create_rename_dict(n_components)
        df_pca = df_pca.rename(columns=columns_rename)

        df_pca[self.table_id] = ids

        return df_pca
    
    def _create_rename_dict(self, n_components)->dict:
        columns_rename = {}
        for i in range(n_components):
            columns_rename[i] = f"{self.features_name}_pca{i}"
        return columns_rename
    

        


# Core for pca 
