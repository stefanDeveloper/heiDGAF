import logging
from enum import Enum

import joblib
import numpy as np
import polars as pl
import torch
from fe_polars.encoding.target_encoding import TargetEncoder
from fe_polars.imputing.base_imputing import Imputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from xgboost import XGBClassifier, XGBRFRegressor

from heidgaf import datasets, models
from heidgaf.cache import DataFrameRedisCache
from heidgaf.datasets import Dataset
from heidgaf.models import Pipeline
from heidgaf.models.lr import LogisticRegression
from heidgaf.feature import Preprocessor


class DNSAnalyzerTraining:
    def __init__(
        self, model: torch.nn.Module, dataset: Dataset = datasets.dgta_dataset
    ) -> None:
        """Trainer class to fit models on data sets.

        Args:
            model (torch.nn.Module): Fit model.
            dataset (heidgaf.dataset.Dataset): Data set for training.
        """
        self.model = model
        self.dataset = dataset

    def train(self, seed=42):
        """Starts training of the model. Checks prior if GPU is available.

        Args:
            seed (int, optional): _description_. Defaults to 42.
        """
        if seed > 0:
            np.random.seed(seed)
            torch.manual_seed(seed)

        # setting device on GPU if available, else CPU
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        logging.info(f"Using device: {device}")
        if torch.cuda.is_available():
            logging.info("GPU detected")
            logging.info(f"\t{torch.cuda.get_device_name(0)}")

        if device.type == "cuda":
            logging.info("Memory Usage:")
            logging.info(
                f"\tAllocated: {round(torch.cuda.memory_allocated(0)/1024**3,1)} GB"
            )
            logging.info(
                f"\tCached:    {round(torch.cuda.memory_reserved(0)/1024**3,1)} GB"
            )

        logging.info(f"Loading data sets")
        
        params = {
            'num_rounds': 100,
            'max_depth': 8,
            'max_leaves': 2**8,
            'alpha': 0.9,
            'eta': 0.1,
            'gamma': 0.1,
            'subsample': 1,
            'reg_lambda': 1,
            'scale_pos_weight': 2,
            'objective': 'binary:logistic',
            'verbose': True,
            'gpu_id': 0,
            'tree_method': 'hist',
            # 'device': 'cuda'
        }

        # Training model
        model_pipeline = Pipeline(
            preprocessor=Preprocessor(
                features_to_drop=[
                    "query",
                    "labels",
                    "thirdleveldomain",
                    "secondleveldomain",
                    "fqdn",
                ]
            ),
            mean_imputer=Imputer(features_to_impute=[], strategy="mean"),
            target_encoder=TargetEncoder(smoothing=100, features_to_encode=[]),
            clf=RandomForestClassifier(),
        )
        
        dataset_full = pl.concat([datasets.dgta_dataset.data, datasets.cic_dataset.data])
        
        dataset_full_data = Dataset(data_path="", data=dataset_full)

        model_pipeline.fit(
            x_train=dataset_full_data.X_train, y_train=dataset_full_data.Y_train
        )
        
        y_pred = model_pipeline.predict(dataset_full_data.X_test)
        logging.info(classification_report(dataset_full_data.Y_test, y_pred, labels=[0,1]))
        
        joblib.dump(model_pipeline.clf, "model.pkl")
