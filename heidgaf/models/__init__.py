import logging
from time import time
from fe_polars.encoding.target_encoding import TargetEncoder
from fe_polars.imputing.base_imputing import Imputer
import numpy as np
import polars as pl
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold

from heidgaf.feature import Preprocessor
from heidgaf.models.lr import LogisticRegression

from xgboost import XGBClassifier, XGBRFClassifier


class Pipeline:
    """Pipeline for training models."""

    def __init__(
        self,
        preprocessor: Preprocessor,
        mean_imputer: Imputer,
        target_encoder: TargetEncoder,
        clf: LogisticRegression,
    ):
        """Initializes preprocessors, encoder, and model.

        Args:
            preprocessor (Preprocessor): Preprocessor to transform input data into features.
            mean_imputer (Imputer): Mean imputer to handle null values.
            target_encoder (TargetEncoder): Target encoder for non-numeric values.
            clf (torch.nn.Modul): torch.nn.Modul for training.
        """
        self.preprocessor = preprocessor
        self.mean_imputer = mean_imputer
        self.target_encoder = target_encoder
        self.clf = clf

    def fit(self, x_train: pl.DataFrame, y_train: pl.DataFrame):
        """Fits models to training data.

        Args:
            x_train (np.array): X data.
            y_train (np.array): Y labels.
        """
        x_train = self.preprocessor.transform(x=x_train)
        x_train = self.target_encoder.fit_transform(x=x_train, y=y_train)
        x_train = self.mean_imputer.fit_transform(x=x_train)
        
        # # Number of trees in random forest
        # n_estimators = [int(x) for x in np.linspace(start = 200, stop = 2000, num = 10)]
        # # Number of features to consider at every split
        # max_features = ['auto', 'sqrt']
        # # Maximum number of levels in tree
        # max_depth = [int(x) for x in np.linspace(10, 110, num = 11)]
        # max_depth.append(None)
        # # Minimum number of samples required to split a node
        # min_samples_split = [2, 5, 10]
        # # Minimum number of samples required at each leaf node
        # min_samples_leaf = [1, 2, 4]
        # # Method of selecting samples for training each tree
        # bootstrap = [True, False]# Create the random grid
        # random_grid = {'n_estimators': n_estimators,
        #             'max_features': max_features,
        #             'max_depth': max_depth,
        #             'min_samples_split': min_samples_split,
        #             'min_samples_leaf': min_samples_leaf,
        #             'bootstrap': bootstrap}

        clf = RandomizedSearchCV(
            self.clf["model"], self.clf["search"], random_state=42, n_iter=100, cv = 3, verbose=2, n_jobs=-1
        )
        start = time()
        model = clf.fit(x_train.to_numpy(), y_train.to_numpy().ravel())
        logging.info(f"GridSearchCV took {time() - start:.2f} seconds for {len(clf.cv_results_['params']):d} candidate parameter settings.")
        logging.info(model.best_params_)

        self.clf = model        
        # self.clf.fit(X=x_train.to_numpy(), y=y_train.to_numpy().ravel())

    def predict(self, x):
        """Predicts given X.

        Args:
            x (np.array): X data

        Returns:
            np.array: Model output.
        """
        x = self.preprocessor.transform(x=x)
        x = self.target_encoder.transform(x=x)
        x = self.mean_imputer.transform(x=x)
        return self.clf.predict(X=x.to_numpy())


xgboost_params = {
    "num_rounds": 100,
    "max_depth": 8,
    "max_leaves": 2**8,
    "alpha": 0.9,
    "eta": 0.1,
    "gamma": 0.1,
    "subsample": 1,
    "reg_lambda": 1,
    "scale_pos_weight": 2,
    "objective": "binary:logistic",
    "verbose": True,
    "gpu_id": 0,
    "tree_method": "hist",
    'device': 'cuda'
}

xgboost_rf_params = {
    "colsample_bynode": 0.8,
    "learning_rate": 1,
    "max_depth": 5,
    "num_parallel_tree": 100,
    "objective": "binary:logistic",
    "subsample": 0.8,
    "tree_method": "hist",
    "booster": "gbtree",
    "device": "cuda",
}

xgboost_model = {
    "model": XGBClassifier(**xgboost_params),
    "search": {
        "max_depth": [3, 6, 9],
        "eta": list(np.linspace(0.1, 0.6, 6)),
        "gamma": [int(x) for x in np.linspace(0, 10, 10)],
    }
}
xgboost_rf_model = XGBRFClassifier(**xgboost_rf_params)
random_forest_model = RandomForestClassifier()
