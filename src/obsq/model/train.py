from .core import ModelPipeline
from ..pipeline import step, PipelineContext, Module
import pandas as pd
import joblib


@step
def train_model(context:PipelineContext):

    # Get data
    con = context.con
    df = con.execute(f"""SELECT* FROM score.main""" ).df()
    df.sort_values(by='gbifID',ascending=True, inplace=True)
    gbifIDs = df['gbifID']
    target = 'score'
    X = df.drop(columns=[target, 'gbifID'])
    y = df.pop(target)
    X.head()

    # Initialize scorer
    model_pipe = ModelPipeline(
        train_size=0.7,
        val_size=0.15,
        test_size=0.15,
        random_state=42
    )

    strat_var = model_pipe.create_stratification_bins(
        df,
        spatial_col='spatial_cluster',  # or use a grid_cell_id if you have one
        species_col='species_encoded',
        time_col='tempo_month',
        n_spatial_bins=6,
        n_time_bins=12
    )

    X_train, X_val, X_test, y_train, y_val, y_test = model_pipe.split_data(X, y, strat_var)

    model_pipe.fit_and_evaluate(X_train, X_val, X_test, y_train, y_val, y_test)
    model_pipe.plot_comparison()
    
    model_pipe.evaluate_test_set(X_test, y_test, model_name='Linear Regression')
    model_pipe.evaluate_test_set(X_test, y_test, model_name='Random Forest')
    model_pipe.evaluate_test_set(X_test, y_test, model_name='XGBoost')

    model_pipe.export_model('Random Forest')

    X_test_out = pd.DataFrame(X_test[:100])
    y_test_out = pd.DataFrame(y_test[:100])

    X_test_out.to_csv('X_test.csv')
    y_test_out.to_csv('y_test.csv')
    

@step
def test_model(context:PipelineContext):

    model = joblib.load('model.joblib')
    print(model)



train_model_module = Module('train', [train_model])



