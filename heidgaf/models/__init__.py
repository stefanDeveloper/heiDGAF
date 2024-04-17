import logging
from time import time
from typing import Any

import numpy as np
import polars as pl
import torch
from fe_polars.encoding.target_encoding import TargetEncoder
from fe_polars.imputing.base_imputing import Imputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV
from xgboost import XGBClassifier, XGBRFClassifier

from heidgaf.feature import Preprocessor


class Pipeline:
    """Pipeline for training models."""

    def __init__(
        self,
        preprocessor: Preprocessor,
        mean_imputer: Imputer,
        target_encoder: TargetEncoder,
        clf: Any,
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
        
        # setting device on GPU if available, else CPU
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logging.info(f"Using device: {self.device}")

        if torch.cuda.is_available():
            logging.info("GPU detected")
            logging.info(f"\t{torch.cuda.get_device_name(0)}")

        if self.device.type == "cuda":
            logging.info("Memory Usage:")
            logging.info(
                f"\tAllocated: {round(torch.cuda.memory_allocated(0)/1024**3,1)} GB"
            )
            logging.info(
                f"\tCached:    {round(torch.cuda.memory_reserved(0)/1024**3,1)} GB"
            )

    def fit(self, x_train: pl.DataFrame, y_train: pl.DataFrame):
        """Fits models to training data.

        Args:
            x_train (np.array): X data.
            y_train (np.array): Y labels.
        """
        x_train = self.preprocessor.transform(x=x_train)
        x_train = self.target_encoder.fit_transform(x=x_train, y=y_train)
        x_train = self.mean_imputer.fit_transform(x=x_train)

        clf = RandomizedSearchCV(
            self.clf["model"],
            self.clf["search"],
            random_state=42,
            n_iter=100,
            cv=3,
            verbose=2,
            n_jobs=-1,
        )
        
        start = time()
        model = clf.fit(x_train.to_numpy(), y_train.to_numpy().ravel())
        logging.info(
            f"GridSearchCV took {time() - start:.2f} seconds for {len(clf.cv_results_['params']):d} candidate parameter settings."
        )
        logging.info(model.best_params_)

        self.clf = model

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
    "max_leaves": 2**8,
    "alpha": 0.9,
    "scale_pos_weight": 0.5,
    "objective": "binary:logistic",
    "tree_method": "hist",
    "device": "cuda",
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
        "eta": list(np.linspace(0.1, 0.6, 6)),
        "gamma": [int(x) for x in np.linspace(0, 10, 10)],
        'learning_rate': [0.03, 0.01, 0.003, 0.001],
        'min_child_weight': [1,3, 5,7, 10],
        'subsample': [0.6, 0.8, 1.0, 1.2, 1.4],
        'colsample_bytree': [0.6, 0.8, 1.0, 1.2, 1.4],
        'max_depth': [3, 4, 5, 6, 7, 8, 9 ,10, 12, 14],
        'reg_lambda':np.array([0.4, 0.6, 0.8, 1, 1.2, 1.4])

    },
}
xgboost_rf_model = {
    "model": XGBRFClassifier(**xgboost_rf_params),
    "search": {
        "max_depth": [3, 6, 9],
        "eta": list(np.linspace(0.1, 0.6, 6)),
        "gamma": [int(x) for x in np.linspace(0, 10, 10)],
    },
}
random_forest_model = {
    "model": RandomForestClassifier(),
    "search": {
        "n_estimators": [int(x) for x in np.linspace(start = 200, stop = 2000, num = 10)],
        "max_features":  ['auto', 'sqrt'],
        "max_depth": [int(x) for x in np.linspace(10, 110, num = 11)].append(None),
        "min_samples_split": [2, 5, 10],
        "min_samples_leaf": [1, 2, 4],
        "bootstrap": [True, False],
    },
}
