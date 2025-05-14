import hashlib
import json
import pickle
import sys
import os
from enum import Enum, unique
import tempfile

import click
import joblib
import numpy as np
import torch
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler
import polars as pl


sys.path.append(os.getcwd())
from src.train.dataset import DatasetLoader
from src.train.model import (
    Pipeline,
)
from src.base.log_config import get_logger
from src.train import CONTEXT_SETTINGS, RESULT_FOLDER, SEED

logger = get_logger("train.train")


def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


@unique
class DatasetEnum(str, Enum):
    ALL = "all"
    COMBINE = "combine"
    CIC = "cic"
    DGTA = "dgta"
    DGARCHIVE = "dgarchive"


@unique
class ModelEnum(str, Enum):
    RANDOM_FOREST_CLASSIFIER = "rf"
    XG_BOOST_CLASSIFIER = "xg"
    GBM_CLASSIFIER = "gbm"


class DetectorTraining:
    def __init__(
        self,
        model_name: ModelEnum.RANDOM_FOREST_CLASSIFIER,
        model_output_path: str = f"./{RESULT_FOLDER}/model",
        dataset: DatasetEnum = DatasetEnum.ALL,
        data_base_path: str = "./data",
        max_rows: int = -1,
    ) -> None:
        """Trainer class to fit models on data sets.

        Args:
            model_name (ModelEnum.RANDOM_FOREST_CLASSIFIER): _description_
            model_output_path (str, optional): _description_. Defaults to "./".
            dataset (DatasetEnum, optional): _description_. Defaults to DatasetEnum.ALL.
            data_base_path (str, optional): _description_. Defaults to "./data".
            max_rows (int, optional): _description_. Defaults to -1.

        Raises:
            NotImplementedError: _description_
        """
        logger.info("Get DatasetLoader.")
        self.dataset_loader = DatasetLoader(base_path=data_base_path, max_rows=max_rows)
        try:
            model_checksum = self._sha256sum(
                os.path.join(model_output_path, f"{model_name}.pickle")
            )
            if model_checksum in model_output_path:
                self.model_checksum = model_checksum
                self.model_output_path = model_output_path
                self.model_output_path = self.model_output_path.replace(
                    self.model_checksum, ""
                )
                self.model_output_path = self.model_output_path.replace(model_name, "")
                self.model_output_path = self.model_output_path.replace("//", "/")
        except:
            logger.warning("Model not found, training starts!")
            self.model_output_path = model_output_path

        logger.info(self.model_output_path)
        self.dataset = []
        match dataset:
            case "combine":
                self.dataset.append(self.dataset_loader.dgta_dataset)
                self.dataset.append(self.dataset_loader.bambenek_dataset)
                self.dataset.append(self.dataset_loader.dga_dataset)
                self.dataset.append(self.dataset_loader.heicloud_dataset)
                self.dataset = self.dataset + self.dataset_loader.dgarchive_dataset
            # CIC DNS does work in practice and data is not clean.
            case "cic":
                self.dataset.append(self.dataset_loader.cic_dataset)
            case "dgta":
                self.dataset.append(self.dataset_loader.dgta_dataset)
            case "dgarchive":
                self.dataset.append(self.dataset_loader.dgarchive_data)
            case _:
                raise NotImplementedError(f"Dataset not implemented!")
        logger.info(f"Set up Pipeline.")
        self.model_name = model_name
        self.scaler = self._load_scaler()
        self.model_pipeline = Pipeline(
            model=self.model_name,
            datasets=self.dataset,
            model_output_path=self.model_output_path,
            scaler=self.scaler,
        )
        self._load_model()

    def explain(self):
        rules = self.model_pipeline.explain(
            self.model_pipeline.x_val, self.model_pipeline.y_val
        )
        save_path = os.path.join(
            self.model_output_path, self.model_name, self.model_checksum
        )
        os.makedirs(save_path, exist_ok=True)

        # Save rules to file
        with open(os.path.join(save_path, "rules.txt"), "w") as f:
            f.write("Extracted Rules:\n\n")
            for i, rule in enumerate(rules, 1):
                f.write(f"Rule {i}: {rule}\n")

    def test(self):
        for X, y, ds in zip(
            self.model_pipeline.ds_X,
            self.model_pipeline.ds_y,
            self.model_pipeline.datasets,
        ):
            logger.info("Test validation test.")
            y_pred = self.model_pipeline.predict(X)
            y_pred = [round(value) for value in y_pred]
            y_labels = np.unique(y).tolist()
            report = classification_report(
                y, y_pred, output_dict=True, labels=y_labels, zero_division=0
            )
            logger.info(report)

            # Get indices of mispredictions
            mispredicted_indices = [
                i for i, (true, pred) in enumerate(zip(y, y_pred)) if true != pred
            ]
            mispredictions = []
            false_pred = []
            # Print or log the mispredicted data points
            for idx in mispredicted_indices:
                error = dict()
                error["y"] = str(y[idx])
                error["y_pred"] = str(y_pred[idx])
                query = str(ds.data[idx].get_column("query").to_list()[0])
                error["query"] = query
                false_pred.append(query)
                mispredictions.append(error)

            # Get matching rows
            matches = ds.data.filter(pl.col("query").is_in(false_pred))
            unique_mispredicton_classes = matches["class"].unique().to_list()

            model_path = os.path.join(
                self.model_output_path, self.model_name, self.model_checksum
            )
            os.makedirs(model_path, exist_ok=True)

            if len(mispredictions) > 0:
                error_report = dict()
                error_report["classes"] = unique_mispredicton_classes
                error_report["mispredictions"] = mispredictions
                with open(os.path.join(model_path, f"errors_{ds.name}.json"), "w") as f:
                    f.write(json.dumps(error_report) + "\n")

            with open(os.path.join(model_path, f"results.json"), "a+") as f:
                results = dict()
                results["ds"] = ds.name
                results["results"] = report
                results["fdr"] = self._fdr(y, y_pred)
                results["fttar"] = self._fttar(y, y_pred)
                f.write(json.dumps(results) + "\n")

    def train(self, seed=SEED):
        """Starts training of the model. Checks prior if GPU is available.

        Args:
            seed (int, optional): _description_. Defaults to 42.
        """
        if seed > 0:
            np.random.seed(seed)
            torch.manual_seed(seed)

        # Training model
        logger.info("Fit model.")
        self.model_pipeline.hyperparam_fit()

        logger.info("Save model")
        self._save_model()
        logger.info("Save scaler")
        self._save_scaler()

        logger.info("Validate test set")
        y_pred = self.model_pipeline.predict(self.model_pipeline.x_test)
        y_pred = [round(value) for value in y_pred]
        logger.info(
            classification_report(self.model_pipeline.y_test, y_pred, labels=[0, 1])
        )

        logger.info("Test model.")
        self.test()

        logger.info("Interpret model.")
        self.explain()

    def _fttar(self, y_actual: list[int], y_pred: list[int]) -> float:
        """FTTAR metric

        Args:
            y_actual (list[int]): y true labels
            y_pred (list[int]): y preds

        Returns:
            float: FTTAR
        """
        _, FP, _, TP = confusion_matrix(y_actual, y_pred).ravel()
        if (TP) == 0:
            logger.debug("WARNING: TP = 0")
            return 0
        return FP / TP

    def _fdr(self, y_actual: list[int], y_pred: list[int]) -> float:
        """Returns False Discovery Rate

        Args:
            y_actual (list[int]): y true labels
            y_pred (list[int]): y preds

        Returns:
            float: FDR
        """
        _, FP, _, TP = confusion_matrix(y_actual, y_pred).ravel()
        if (FP + TP) == 0:
            logger.debug("WARNING: FP + TP = 0")
            return 0
        return FP / (FP + TP)

    def _load_model(self):
        try:
            model_path = os.path.join(
                self.model_output_path,
                self.model_name,
                self.model_checksum,
                f"{self.model_name}.pickle",
            )
            with open(model_path, "rb") as input_file:
                self.model_pipeline.model.clf = pickle.load(input_file)
        except:
            logger.warning(
                f"Model could not be loaded. Model path is '{self.model_output_path}' or path incorrect."
            )

    def _save_model(self):
        logger.info("Save trained model to a file.")
        with open(
            os.path.join(
                tempfile.gettempdir(), f"rf_{self.model_pipeline.trial.number}.pickle"
            ),
            "wb",
        ) as fout:
            pickle.dump(self.model_pipeline.model.clf, fout)

        self.model_checksum = self._sha256sum(
            os.path.join(
                tempfile.gettempdir(), f"rf_{self.model_pipeline.trial.number}.pickle"
            )
        )
        model_path = os.path.join(
            self.model_output_path, self.model_name, self.model_checksum
        )
        os.makedirs(model_path, exist_ok=True)
        with open(os.path.join(model_path, f"{self.model_name}.pickle"), "wb") as fout:
            pickle.dump(self.model_pipeline.model.clf, fout)

    def _load_scaler(self):
        try:
            scaler_path = os.path.join(
                self.model_output_path,
                self.model_name,
                self.model_checksum,
                "scaler.pickle",
            )
            scaler = joblib.load(scaler_path)
            logger.info("Scaler loaded successfully.")
            return scaler
        except:
            logger.warning(
                f"Scaler file not found. Model path is '{self.model_output_path}' or path incorrect."
            )
            return StandardScaler()

    def _save_scaler(self):
        """
        Save the scaler for future use.
        """
        scaler_path = os.path.join(
            self.model_output_path, self.model_name, self.model_checksum
        )
        os.makedirs(scaler_path, exist_ok=True)
        with open(os.path.join(scaler_path, "scaler.pickle"), "wb") as f:
            pickle.dump(self.scaler, f)

    def _sha256sum(self, file_path: str) -> str:
        """Return a SHA265 sum check to validate the model.

        Args:
            file_path (str): File path of model.

        Returns:
            str: SHA256 sum
        """
        h = hashlib.sha256()

        with open(file_path, "rb") as file:
            while True:
                # Reading is buffered, so we can read smaller chunks.
                chunk = file.read(h.block_size)
                if not chunk:
                    break
                h.update(chunk)

        return h.hexdigest()


_ds_options = [
    click.option(
        "--dataset",
        "dataset",
        default="combine",
        type=click.Choice(["combine", "dgarchive", "cic", "dgta"]),
        help="Data set to train model, choose between all available datasets, DGArchive, CIC and DGTA.",
    ),
    click.option(
        "--dataset_path",
        "dataset_path",
        type=click.Path(exists=True),
        help="Dataset path, follow folder structure.",
    ),
    click.option(
        "--dataset_max_rows",
        "dataset_max_rows",
        default=-1,
        type=int,
        help="Maximum rows to load from each dataset.",
    ),
]


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    click.secho("Train heiDGAF CLI")


@cli.command()
@add_options(_ds_options)
@click.option(
    "--model",
    "model",
    type=click.Choice(["xg", "rf", "gbm"]),
    help="Model to train, choose between XGBoost and RandomForest classifier",
)
@click.option(
    "--model_output_path",
    "model_output_path",
    type=click.Path(),
    default=f"./{RESULT_FOLDER}/model",
    help="Model output path. Stores model with {{MODEL}}_{{SHA256}}.pickle.",
)
def train(
    dataset: str,
    dataset_path: str,
    dataset_max_rows: int,
    model: str,
    model_output_path: str,
) -> None:
    trainer = DetectorTraining(
        model_name=model,
        dataset=dataset,
        data_base_path=dataset_path,
        max_rows=dataset_max_rows,
        model_output_path=model_output_path,
    )
    trainer.train()


@cli.command()
@add_options(_ds_options)
@click.option(
    "--model",
    "model",
    type=click.Choice(["xg", "rf", "gbm"]),
    help="Model to train, choose between XGBoost and RandomForest classifier",
)
@click.option(
    "--model_path",
    "model_path",
    type=click.Path(exists=True),
    help="Model path.",
)
def test(
    dataset: str,
    dataset_path: str,
    dataset_max_rows: int,
    model: str,
    model_path: str,
) -> None:
    trainer = DetectorTraining(
        dataset=dataset,
        data_base_path=dataset_path,
        max_rows=dataset_max_rows,
        model_output_path=model_path,
        model_name=model,
    )
    trainer.test()


@cli.command()
@add_options(_ds_options)
@click.option(
    "--model",
    "model",
    type=click.Choice(["xg", "rf", "gbm"]),
    help="Model to train, choose between XGBoost and RandomForest classifier",
)
@click.option(
    "--model_path",
    "model_path",
    type=click.Path(exists=True),
    help="Model path.",
)
def explain(
    dataset: str,
    dataset_path: str,
    dataset_max_rows: int,
    model: str,
    model_path: str,
) -> None:
    trainer = DetectorTraining(
        dataset=dataset,
        data_base_path=dataset_path,
        max_rows=dataset_max_rows,
        model_output_path=model_path,
        model_name=model,
    )
    trainer.explain()


if __name__ == "__main__":  # pragma: no cover
    cli()
