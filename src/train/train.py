import logging
import sys
import os
from enum import Enum, unique

import joblib
import numpy as np
import polars as pl
import torch
from sklearn.metrics import classification_report

sys.path.append(os.getcwd())
from src.train.dataset import Dataset, DatasetLoader, Dataset
from src.train.feature import Processor
from src.train.model import (
    Pipeline,
    random_forest_model,
    xgboost_model,
    xgboost_rf_model,
)
from src.base.log_config import get_logger

logger = get_logger("train.train")


@unique
class DatasetEnum(str, Enum):
    ALL = "all"
    CIC = "cic"
    DGTA = "dgta"
    DGARCHIVE = "dgarchive"


@unique
class ModelEnum(str, Enum):
    RANDOM_FOREST_CLASSIFIER = "rf"
    XG_BOOST_CLASSIFIER = "xg"
    XG_BOOST_RANDOM_FOREST_CLASSIFIER = "xg-rf"


class DetectorTraining:
    def __init__(
        self,
        model: ModelEnum.RANDOM_FOREST_CLASSIFIER,
        dataset: DatasetEnum = DatasetEnum.ALL,
        data_base_path: str = "./data",
        max_rows: int = -1,
    ) -> None:
        """Trainer class to fit models on data sets.

        Args:
            model (torch.nn.Module): Fit model.
            dataset (heidgaf.datasets.Dataset): Data set for training.
        """
        logger.info("Get DatasetLoader.")
        self.datasets = DatasetLoader(base_path=data_base_path, max_rows=100)
        match dataset:
            case "all":
                self.dataset = Dataset(
                    data_path="",
                    data=pl.concat(
                        [
                            self.datasets.dgta_dataset.data,
                            self.datasets.cic_dataset.data,
                            self.datasets.bambenek_dataset.data,
                            self.datasets.dga_dataset.data,
                            self.datasets.dgarchive_data.data,
                        ]
                    ),
                    max_rows=100,
                )
            case "cic":
                self.dataset = self.datasets.cic_dataset
            case "dgta":
                self.dataset = self.datasets.dgta_dataset
            case "dgarchive":
                self.dataset = self.datasets.dgarchive_data
            case _:
                raise NotImplementedError(f"Dataset not implemented!")

        match model:
            case "rf":
                self.model = random_forest_model
            case "xg":
                self.model = xgboost_model
            case "xg-rf":
                self.model = xgboost_rf_model
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

        # Training model
        logger.info(f"Set up Pipeline.")
        model_pipeline = Pipeline(
            preprocessor=Processor(
                features_to_drop=[
                    "query",
                    "labels",
                    "thirdleveldomain",
                    "secondleveldomain",
                    "fqdn",
                    "tld",
                ]
            ),
            clf=self.model,
        )

        logger.info("Fit model.")
        model_pipeline.fit(x_train=self.dataset.X_train, y_train=self.dataset.Y_train)

        logger.info("Validate test set")
        y_pred = model_pipeline.predict(self.dataset.X_test)
        logger.info(classification_report(self.dataset.Y_test, y_pred, labels=[0, 1]))

        logger.info("Test validation test.")
        y_pred = model_pipeline.predict(self.dataset.X_val)
        logger.info(classification_report(self.dataset.Y_val, y_pred, labels=[0, 1]))

        # TODO Dump models (XGBoost must be saved differently)
        joblib.dump(model_pipeline.clf, output_path)


if __name__ == "__main__":  # pragma: no cover
    name = sys.argv[1]
    ds = sys.argv[2]
    output_train_path = sys.argv[3]

    trainer = DetectorTraining(model=name, dataset=ds)
    trainer.train()
