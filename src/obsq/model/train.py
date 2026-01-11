from .core import ObservationQualityScorer
from ..pipeline import step, PipelineContext, Module

@step
def train_model(context:PipelineContext):
    # Initialize scorer
    scorer = ObservationQualityScorer(
        train_size=0.7,
        val_size=0.15,
        test_size=0.15,
        random_state=42
    )
    # Get data
    con = context.con
    df = con.execute(f"""SELECT* FROM features.combined""" ).df()
    df = df.set_index('gbifID')

    X = df.drop(columns=['expert_match'])
    y = df['expert_match']

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
    scorer.evaluate_test_set(X_test, y_test, model_name='XGBoost')



train_model_module = Module('train', [train_model])

