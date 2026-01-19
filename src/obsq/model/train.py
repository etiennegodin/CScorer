from .core import ObservationQualityScorer
from ..pipeline import step, PipelineContext, Module

@step
def train_model(context:PipelineContext):

    # Get data
    con = context.con
    df = con.execute(f"""SELECT* FROM score.main""" ).df()
    #df = df.set_index('gbifID')
    target = 'score'
    X = df.drop(columns=[target, 'gbifID'])
    y = df.pop(target)
    X.head()


    # Initialize scorer
    scorer = ObservationQualityScorer(
        train_size=0.7,
        val_size=0.15,
        test_size=0.15,
        random_state=42
    )

    strat_var = scorer.create_stratification_bins(
        df,
        spatial_col='spatial_cluster',  # or use a grid_cell_id if you have one
        species_col='species_encoded',
        time_col='tempo_month',
        n_spatial_bins=6,
        n_time_bins=12
    )

    X_train, X_val, X_test, y_train, y_val, y_test = scorer.split_data(X, y, strat_var)

    scorer.fit_and_evaluate(X_train, X_val, X_test, y_train, y_val, y_test)
    scorer.plot_comparison()
    scorer.evaluate_test_set(X_test, y_test, model_name='Linear Regression')
    scorer.evaluate_test_set(X_test, y_test, model_name='Random Forest')
    scorer.evaluate_test_set(X_test, y_test, model_name='XGBoost')

train_model_module = Module('train', [train_model])

