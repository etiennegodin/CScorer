



import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import (
    classification_report, 
    confusion_matrix, 
    roc_auc_score,
    precision_recall_curve,
    auc,
    f1_score
)
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns

class ObservationQualityScorer:
    """
    Pipeline for building observation quality scorer with multi-feature stratification
    """
    
    def __init__(self, train_size=0.7, val_size=0.15, test_size=0.15, random_state=42):
        """
        Parameters:
        -----------
        train_size : float, proportion for training set
        val_size : float, proportion for validation set
        test_size : float, proportion for test set (should sum to 1.0)
        random_state : int, for reproducibility
        """
        assert abs(train_size + val_size + test_size - 1.0) < 1e-6, "Splits must sum to 1.0"
        
        self.train_size = train_size
        self.val_size = val_size
        self.test_size = test_size
        self.random_state = random_state
        self.scaler = StandardScaler()
        self.models = {}
        self.results = {}
        
    def create_stratification_bins(self, df, spatial_col, species_col, time_col, 
                                   n_spatial_bins=10, n_time_bins=12):
        """
        Create composite stratification variable from spatial, species, and temporal features
        
        Parameters:
        -----------
        df : pd.DataFrame, your dataset
        spatial_col : str, name of spatial feature (e.g., 'latitude' or 'grid_cell_id')
        species_col : str, name of species column
        time_col : str, name of temporal feature (e.g., 'month' or 'year')
        n_spatial_bins : int, number of spatial bins to create
        n_time_bins : int, number of temporal bins to create
        
        Returns:
        --------
        pd.Series with stratification labels
        """
        # Create spatial bins (quantile-based to ensure even distribution)
        spatial_bins = pd.qcut(df[spatial_col], q=n_spatial_bins, labels=False, duplicates='drop')
        
        # Create temporal bins
        if df[time_col].dtype in ['int64', 'float64']:
            time_bins = pd.qcut(df[time_col], q=n_time_bins, labels=False, duplicates='drop')
        else:
            # If already categorical (e.g., month names)
            time_bins = pd.factorize(df[time_col])[0]
        
        # For species, keep as-is if manageable number of categories
        # Otherwise bin rare species together
        species_counts = df[species_col].value_counts()
        rare_threshold = len(df) * 0.01  # Species with <1% of observations
        species_mapped = df[species_col].apply(
            lambda x: x if species_counts[x] >= rare_threshold else 'rare_species'
        )
        species_labels = pd.factorize(species_mapped)[0]
        
        # Combine into single stratification variable
        strat_var = (
            spatial_bins.astype(str) + '_' + 
            species_labels.astype(str) + '_' + 
            time_bins.astype(str)
        )
        
        # Handle strata with too few samples (minimum 2 per stratum for splitting)
        strat_counts = strat_var.value_counts()
        small_strata = strat_counts[strat_counts < 2].index
        strat_var = strat_var.apply(
            lambda x: 'mixed_stratum' if x in small_strata else x
        )
        
        return strat_var
    
    def split_data(self, X, y, stratification_var):
        """
        Perform stratified train-validation-test split
        
        Parameters:
        -----------
        X : pd.DataFrame, features
        y : pd.Series, target variable
        stratification_var : pd.Series, composite stratification variable
        
        Returns:
        --------
        X_train, X_val, X_test, y_train, y_val, y_test
        """
        # First split: separate test set
        test_ratio = self.test_size
        X_temp, X_test, y_temp, y_test = train_test_split(
            X, y, 
            test_size=test_ratio,
            stratify=stratification_var,
            random_state=self.random_state
        )
        
        # Get stratification var for temp set
        strat_temp = stratification_var.loc[X_temp.index]
        
        # Second split: separate train and validation
        val_ratio = self.val_size / (self.train_size + self.val_size)
        X_train, X_val, y_train, y_val = train_test_split(
            X_temp, y_temp,
            test_size=val_ratio,
            stratify=strat_temp,
            random_state=self.random_state
        )
        
        print(f"Dataset split:")
        print(f"  Training: {len(X_train)} samples ({len(X_train)/len(X)*100:.1f}%)")
        print(f"  Validation: {len(X_val)} samples ({len(X_val)/len(X)*100:.1f}%)")
        print(f"  Test: {len(X_test)} samples ({len(X_test)/len(X)*100:.1f}%)")
        print(f"\nClass distribution:")
        print(f"  Train: {y_train.mean():.3f} positive")
        print(f"  Val: {y_val.mean():.3f} positive")
        print(f"  Test: {y_test.mean():.3f} positive")
        
        return X_train, X_val, X_test, y_train, y_val, y_test
    
    def fit_and_evaluate(self, X_train, X_val, X_test, y_train, y_val, y_test,
                        scale_features=True):
        """
        Train all three models and evaluate on validation set
        
        Parameters:
        -----------
        X_train, X_val, X_test : feature matrices
        y_train, y_val, y_test : target vectors
        scale_features : bool, whether to standardize features (recommended for LogReg)
        """
        # Scale features if requested
        if scale_features:
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_val_scaled = self.scaler.transform(X_val)
            X_test_scaled = self.scaler.transform(X_test)
        else:
            X_train_scaled = X_train
            X_val_scaled = X_val
            X_test_scaled = X_test
        
        # Calculate class weights for imbalanced data
        n_samples = len(y_train)
        n_pos = y_train.sum()
        n_neg = n_samples - n_pos
        scale_pos_weight = n_neg / n_pos
        
        # Define models with balanced class weights
        models = {}
            'Random Forest': RandomForestClassifier(
                n_estimators=100,
                class_weight='balanced',
                max_depth=10,
                random_state=self.random_state,
                n_jobs=-1
            ),
            'XGBoost': XGBClassifier(
                n_estimators=100,
                max_depth=6,
                scale_pos_weight=scale_pos_weight,
                learning_rate=0.1,
                random_state=self.random_state,
                eval_metric='logloss'
            )
        }
        
        # Train and evaluate each model
        for name, model in models.items():
            print(f"\n{'='*60}")
            print(f"Training {name}...")
            print(f"{'='*60}")
            
            # Use scaled data for LogReg, original for tree-based
            if name == 'Logistic Regression':
                model.fit(X_train_scaled, y_train)
                y_val_pred = model.predict(X_val_scaled)
                y_val_proba = model.predict_proba(X_val_scaled)[:, 1]
            else:
                model.fit(X_train, y_train)
                y_val_pred = model.predict(X_val)
                y_val_proba = model.predict_proba(X_val)[:, 1]
            
            # Calculate metrics
            val_metrics = self._calculate_metrics(y_val, y_val_pred, y_val_proba)
            
            # Store model and results
            self.models[name] = model
            self.results[name] = {
                'validation_metrics': val_metrics,
                'y_val_pred': y_val_pred,
                'y_val_proba': y_val_proba
            }
            
            # Print results
            print(f"\nValidation Results:")
            print(f"  ROC-AUC: {val_metrics['roc_auc']:.4f}")
            print(f"  PR-AUC: {val_metrics['pr_auc']:.4f}")
            print(f"  F1 Score: {val_metrics['f1']:.4f}")
            print(f"\nClassification Report:")
            print(val_metrics['classification_report'])
    
    def _calculate_metrics(self, y_true, y_pred, y_proba):
        """Calculate comprehensive metrics for imbalanced classification"""
        precision, recall, _ = precision_recall_curve(y_true, y_proba)
        
        return {
            'roc_auc': roc_auc_score(y_true, y_proba),
            'pr_auc': auc(recall, precision),
            'f1': f1_score(y_true, y_pred),
            'classification_report': classification_report(y_true, y_pred),
            'confusion_matrix': confusion_matrix(y_true, y_pred)
        }
    
    def evaluate_test_set(self, X_test, y_test, model_name, scale_features=True):
        """
        Evaluate final model on held-out test set
        
        Parameters:
        -----------
        X_test : test features
        y_test : test labels
        model_name : str, which model to use
        scale_features : bool, whether to scale (use True for LogReg)
        """
        if model_name not in self.models:
            raise ValueError(f"Model {model_name} not found. Available: {list(self.models.keys())}")
        
        model = self.models[model_name]
        
        if model_name == 'Logistic Regression' and scale_features:
            X_test = self.scaler.transform(X_test)
        
        y_test_pred = model.predict(X_test)
        y_test_proba = model.predict_proba(X_test)[:, 1]
        
        test_metrics = self._calculate_metrics(y_test, y_test_pred, y_test_proba)
        
        print(f"\n{'='*60}")
        print(f"TEST SET RESULTS - {model_name}")
        print(f"{'='*60}")
        print(f"ROC-AUC: {test_metrics['roc_auc']:.4f}")
        print(f"PR-AUC: {test_metrics['pr_auc']:.4f}")
        print(f"F1 Score: {test_metrics['f1']:.4f}")
        print(f"\nClassification Report:")
        print(test_metrics['classification_report'])
        
        return test_metrics
    
    def plot_comparison(self):
        """Plot comparison of models on validation set"""
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        
        # Plot 1: ROC-AUC and PR-AUC comparison
        metrics_data = []
        for name, results in self.results.items():
            metrics_data.append({
                'Model': name,
                'ROC-AUC': results['validation_metrics']['roc_auc'],
                'PR-AUC': results['validation_metrics']['pr_auc']
            })
        
        metrics_df = pd.DataFrame(metrics_data)
        metrics_df_melted = metrics_df.melt(id_vars='Model', var_name='Metric', value_name='Score')
        
        sns.barplot(data=metrics_df_melted, x='Model', y='Score', hue='Metric', ax=axes[0])
        axes[0].set_title('Model Comparison: AUC Metrics')
        axes[0].set_ylim(0, 1)
        axes[0].legend(title='Metric')
        
        # Plot 2: Confusion matrices
        n_models = len(self.results)
        confusion_fig, confusion_axes = plt.subplots(1, n_models, figsize=(5*n_models, 4))
        
        if n_models == 1:
            confusion_axes = [confusion_axes]
        
        for idx, (name, results) in enumerate(self.results.items()):
            cm = results['validation_metrics']['confusion_matrix']
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=confusion_axes[idx])
            confusion_axes[idx].set_title(f'{name}\nConfusion Matrix')
            confusion_axes[idx].set_ylabel('True Label')
            confusion_axes[idx].set_xlabel('Predicted Label')
        
        plt.tight_layout()
        #plt.show()
        plt.savefig('save.png')
