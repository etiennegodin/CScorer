from ...pipeline import PipelineContext, ClassStep
from sklearn.decomposition import PCA
import numpy as np
import pandas as pd
from typing import Union, Literal, Dict, get_args

TRANSFORMER_TYPES = Literal["linear", "log"]
COMBINE_TYPE = Literal['inner','left', 'right', 'outer', 'cross']
ID_COLS = Literal['gbifID', 'recordedBy', 'taxonID', 'identifiedBy']


class Encoder(ClassStep):
    def __init__(self, name, table_id:str = 'gbifID',
                  pca_variance_threshold = 0.99,
                  factorize_rare_threshold = 0.01, 
                  **kwargs):
        """
        Pca wrapper
        """
        self.features_name = name
        self.table_id = table_id
        self.input_table_name = f"transformed.{name}"
        self.output_table_name = f"encoded.{name}"
        self.pca_variance_threshold = pca_variance_threshold
        self.factorize_rare_threshold = factorize_rare_threshold
        super().__init__(name, retry_attempts =1,**kwargs)


    def _execute(self, context):
        df_encoded = None
        df_pca = None

        #Load df
        try:
            df = context.con.execute(f"SELECT * FROM {self.input_table_name}").df()
        except Exception as e:
            self.logger.warning(f"Error loading features from {self.input_table_name}. Trying with default features")
            self.input_table_name = f'features.{self.features_name}'
            try:
                df = context.con.execute(f"SELECT * FROM {self.input_table_name}").df()
            except Exception as e:
                self.logger.error(f"Couldnt load data from {self.input_table_name} : \n{e}")

        df_out = pd.DataFrame(df[self.table_id])
        
        # Find highligh correlated features 
        to_reduce = self._find_correlated(df)
        if len(to_reduce) > 0:
            #Reduce with pca
            df_pca = self.pca_reducer(df, to_reduce)
            if df_pca is not None: 
                df_out = pd.merge(df_out, df_pca, on = self.table_id)
        else:
            df_out = df

        #Factorize categorical features 
        df_out = self.factorize_categorical_features(df_out)

        context.con.execute(f"CREATE OR REPLACE TABLE {self.output_table_name} AS SELECT * FROM df_out")

        return super()._execute(context)
        

    def _find_correlated(self, df:pd.DataFrame)->list:
        df = df.drop(columns=[self.table_id])
        corr = df.corr(numeric_only=True)
        upper_tri = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
        return [column for column in upper_tri.columns if any(upper_tri[column].abs() > 0.8)]
    
    def pca_reducer(self, df, features_list:list)->pd.DataFrame:
        variance = 0.0
        n_components = 1

        ids = df[self.table_id]
        df_pruned = df[features_list]

        #Iterate until variance explained
        for i in range(len(features_list)):
            if variance > self.pca_variance_threshold:
                break
            n_components += 1
            pca = PCA(n_components=n_components)
            pca.fit(df_pruned)
            variance = sum(pca.explained_variance_ratio_)

        self.logger.info(f'\tRan pca on {self.features_name} | {n_components} components explained {round(variance,3)}% of variance')

        #Transform original data 
        df_pca = pd.DataFrame(pca.transform(df_pruned))

        #Rename
        columns_rename = self._create_rename_dict(n_components)
        df_pca = df_pca.rename(columns=columns_rename)

        df_pca[self.table_id] = ids

        return df_pca

    def factorize_categorical_features(self, df:pd.DataFrame, to_ignore:Union[list,str] = None)->pd.DataFrame:
        ids = df[self.table_id]
        df_cat = df.select_dtypes(include='object')
        df_numeric = df.select_dtypes(include='number')

        #Drop id column if object
        if self.table_id in df_cat.columns:
            df_cat.drop(columns=[self.table_id], inplace=True)

        #Drop to ignore features 
        if to_ignore is not None:
            if not isinstance(to_ignore, list):
                try:
                    to_ignore = [to_ignore]
                except Exception as e:
                    raise e
            df_cat = df_cat.drop(columns=to_ignore)

        if len(df_cat.columns.to_list()) < 1 :
            return df
         
        #Classify as rare if n_count < threshold, factorize 
        for c in df_cat.columns:
            unique_counts = df_cat[c].value_counts()
            rare_threshold = len(df_cat) * self.factorize_rare_threshold  # Uniques with <x% of observations
            df[c] = df[c].apply(lambda x: x if unique_counts[x] >= rare_threshold else 'rare')
            codes, uniques = pd.factorize(df_cat[c])  
            df[f'{c}_encoded'] = codes
            df = df.drop(columns=[c])
            self.logger.info(f"\tFactorized categorical feature '{c}' from {self.features_name}")
    
        return df

    def _create_rename_dict(self, n_components)->dict:
        columns_rename = {}
        for i in range(n_components):
            columns_rename[i] = f"{self.features_name}_pca{i}"
        return columns_rename
    
class Transformer(ClassStep):
    def __init__(self, name, transform_dict:Dict[TRANSFORMER_TYPES,list[str]], table_id = 'gbifID', **kwargs):
        self.features_name = name
        self.transform_dict = transform_dict
        self.table_id = table_id
        self.input_table_name = f"features.{name}"
        self.output_table_name = f"transformed.{name}"
        super().__init__(name, **kwargs)

    def _execute(self, context):
        df = context.con.execute(f"SELECT * FROM {self.input_table_name}").df()
        for type, cols in self.transform_dict.items():
            if cols == '*':
                cols = df.columns.to_list()
                cols.remove(self.table_id)
            if type == 'linear':
                df[cols] = (df[cols] - df[cols].min()) / (df[cols].max() - df[cols].min())
            if type == 'log':
                df[cols] = np.log1p(df[cols])

        context.con.execute(f"CREATE OR REPLACE TABLE {self.output_table_name} AS SELECT * FROM df")
        return super()._execute(context)
    
    
class Combine(ClassStep):
    def __init__(self, name, combine_dict:list[tuple[str,ID_COLS,COMBINE_TYPE]],  **kwargs):
        self.combine_dict = combine_dict
        super().__init__(name, **kwargs)

    def _execute(self, context):
        schema_list = ['encoded', 'transformed', 'features']
        #Load df
        init_table = "preprocessed.gbif_citizen"
        id_cols = list(get_args(ID_COLS))
        id_string = ""
        for id in id_cols:
            id_string += id
            id_string += ','

        df_out = context.con.execute(f"SELECT {id_string} FROM {init_table} ").df()
        print(df_out)
        for (feature,key, how) in self.combine_dict:
            for schema in schema_list:
                table_name = f"{schema}.{feature}"
                try:
                    df = context.con.execute(f"SELECT * FROM {table_name}").df()
                    break
                except Exception as e:
                    self.logger.warning(f"Error loading features from {table_name}. Trying with next set of features")

            df_out = pd.merge(df_out, df, on = key, how = how)
        print(df_out)
        df_out.drop(columns=id_cols[1:], inplace=True)
        context.con.execute(f"CREATE OR REPLACE TABLE {'features.combined_p'} AS SELECT * FROM df_out")

        return super()._execute(context)
    
        