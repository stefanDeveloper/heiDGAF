import logging
from enum import Enum, unique

import joblib
import numpy as np
import polars as pl
import torch
from fe_polars.encoding.target_encoding import TargetEncoder
from fe_polars.imputing.base_imputing import Imputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

from heidgaf import datasets, models
from heidgaf.datasets import Dataset
from heidgaf.feature import Preprocessor
from heidgaf.models import Pipeline


@unique
class Dataset(str, Enum):
    ALL = "all"
    CIC = "cic"
    DGTA = "dgta"


@unique
class Model(str, Enum):
    RANDOM_FOREST_CLASSIFIER = "rf"
    XG_BOOST_CLASSIFIER = "xg"
    XG_BOOST_RANDOM_FOREST_CLASSIFIER = "xg-rf"


class DNSAnalyzerTraining:
    def __init__(
        self, model: Model.RANDOM_FOREST_CLASSIFIER, dataset: Dataset = Dataset.ALL
    ) -> None:
        """Trainer class to fit models on data sets.

        Args:
            model (torch.nn.Module): Fit model.
            dataset (heidgaf.dataset.Dataset): Data set for training.
        """
        match dataset:
            case "all":
                self.dataset = datasets.Dataset(
                    data_path="",
                    data=pl.concat(
                        [datasets.dgta_dataset.data, datasets.cic_dataset.data, datasets.bambenek_dataset.data, datasets.dga_dataset.data]
                    ),
                )
            case "cic":
                self.dataset = datasets.cic_dataset.data
            case "dgta":
                self.dataset = datasets.dgta_dataset.data
            case _:
                raise NotImplementedError(f"Dataset not implemented!")

        match model:
            case "rf":
                self.model = models.random_forest_model
            case "xg":
                self.model = models.xgboost_model
            case "xg-rf":
                self.model = models.xgboost_rf_model
            case _:
                raise NotImplementedError(f"Model not implemented!")

    def train(self, seed=42, output_path: str = "model.pkl"):
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
            mean_imputer=Imputer(features_to_impute=["thirdleveldomain_full_count", "secondleveldomain_full_count", "fqdn_full_count"], strategy="mean"),
            target_encoder=TargetEncoder(smoothing=100, features_to_encode=["tld"]),
            clf=self.model,
        )

        model_pipeline.fit(x_train=self.dataset.X_train, y_train=self.dataset.Y_train)

        y_pred = model_pipeline.predict(self.dataset.X_test)
        logging.info(classification_report(self.dataset.Y_test, y_pred, labels=[0, 1]))

        y_pred = model_pipeline.predict(self.dataset.X_val)
        logging.info(classification_report(self.dataset.Y_val, y_pred, labels=[0, 1]))

        joblib.dump(model_pipeline.clf, output_path)
