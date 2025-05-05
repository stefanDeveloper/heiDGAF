import argparse
import pickle
import re
import sys
import os
from enum import Enum, unique

import click
import numpy as np
import polars as pl
import torch
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler

sys.path.append(os.getcwd())
from src.train.dataset import Dataset, DatasetLoader, Dataset
from src.train.feature import Processor
from src.train.model import (
    Pipeline,
)
from src.base.log_config import get_logger
from src.train.explainer import PC

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


@unique
class TypeEnum(str, Enum):
    EXPLAIN = "explain"
    TRAIN = "train"


class DetectorTraining:
    def __init__(
        self,
        type: TypeEnum.TRAIN,
        model: ModelEnum.RANDOM_FOREST_CLASSIFIER,
        model_output_path: str = "./",
        dataset: DatasetEnum = DatasetEnum.ALL,
        data_base_path: str = "./data",
        max_rows: int = -1,
    ) -> None:
        """Trainer class to fit models on data sets.

        Args:
            model (torch.nn.Module): Fit model.
            dataset (src.train.datasets.Dataset): Data set for training.
            data_base_path(src.train.train.DatasetEnum):
        """
        logger.info("Get DatasetLoader.")
        self.datasets = DatasetLoader(base_path=data_base_path, max_rows=max_rows)
        self.model_output_path = model_output_path
        self.type = type
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
                            self.datasets.dgarchive_dataset.data,
                        ]
                    ),
                    max_rows=max_rows,
                )
            case "cic":
                self.dataset = self.datasets.cic_dataset
            case "dgta":
                self.dataset = self.datasets.dgta_dataset
            case "dgarchive":
                self.dataset = self.datasets.dgarchive_data
            case _:
                raise NotImplementedError(f"Dataset not implemented!")
        self.model = model

    def explain(self):
        model_pipeline = Pipeline(
            processor=Processor(
                features_to_drop=[
                    "query",
                    "labels",
                    "thirdleveldomain",
                    "secondleveldomain",
                    "fqdn",
                    "tld",
                ]
            ),
            model=self.model,
            scaler=StandardScaler(),
            dataset=self.dataset,
            model_output_path=self.model_output_path,
        )
        model_pipeline.explain(model_pipeline.x_test, model_pipeline.y_test)

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
            processor=Processor(
                features_to_drop=[
                    "query",
                    "labels",
                    "thirdleveldomain",
                    "secondleveldomain",
                    "fqdn",
                    "tld",
                ]
            ),
            model=self.model,
            dataset=self.dataset,
            model_output_path=self.model_output_path,
            scaler=StandardScaler(),
        )

        logger.info("Fit model.")
        model_pipeline.fit()

        logger.info("Validate test set")
        y_pred = model_pipeline.predict(model_pipeline.x_test)
        y_pred = [round(value) for value in y_pred]
        logger.info(classification_report(model_pipeline.y_test, y_pred, labels=[0, 1]))

        logger.info("Test validation test.")
        y_pred = model_pipeline.predict(model_pipeline.x_val)
        y_pred = [round(value) for value in y_pred]
        logger.info(classification_report(model_pipeline.y_val, y_pred, labels=[0, 1]))

        logger.info("Interpret model.")
        model_pipeline.explain(model_pipeline.x_val, model_pipeline.y_val)

    def _save_scaler(scaler, model_type):
        """
        Save the scaler for future use.

        Args:
            scaler: Fitted StandardScaler object
            model_type (str): Type of model being used
        """
        os.makedirs(f"./models/{model_type}", exist_ok=True)
        with open(f"./models/{model_type}/scaler.pickle", "wb") as f:
            pickle.dump(scaler, f)


@click.command()
@click.option(
    "-t",
    "--type",
    type=click.Choice(["explain", "train"]),
    help="Type to explain data or train classifier",
)
@click.option(
    "-m",
    "--model",
    type=click.Choice(["xg", "rf"]),
    help="Model to train, choose between XGBoost and RandomForest classifier",
)
@click.option(
    "-ds",
    "--dataset",
    default="all",
    type=click.Choice(["all", "dgarchive", "cic", "dgta"]),
    help="Data set to train model, choose between all available datasets, DGArchive, CIC and DGTA.",
)
@click.option(
    "-ds_path",
    "--dataset_path",
    type=click.Path(exists=True),
    help="Dataset path, follow folder structure.",
)
@click.option(
    "-ds_max_rows",
    "--dataset_max_rows",
    default=-1,
    type=int,
    help="Maximum rows to load from each dataset.",
)
@click.option(
    "-m_output_path",
    "--model_output_path",
    type=click.Path(exists=True),
    help="Model output path. Stores model with {{MODEL}}_{{SHA256}}.pickle.",
)
def main(type, model, dataset, dataset_path, dataset_max_rows, model_output_path):
    trainer = DetectorTraining(
        type=type,
        model=model,
        dataset=dataset,
        data_base_path=dataset_path,
        max_rows=dataset_max_rows,
        model_output_path=model_output_path,
    )
    if type == TypeEnum.TRAIN:
        trainer.train()
    elif type == TypeEnum.EXPLAIN:
        trainer.explain()


if __name__ == "__main__":  # pragma: no cover
    main()
