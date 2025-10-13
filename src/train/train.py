import hashlib
import json
import os
import pickle
import sys
import tempfile
from enum import Enum, unique

import click
import joblib
import numpy as np
import polars as pl
import torch
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler

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
    """Available dataset configurations for DGA detection model training"""

    COMBINE = "combine"
    CIC = "cic"
    DGTA = "dgta"
    DGARCHIVE = "dgarchive"


@unique
class ModelEnum(str, Enum):
    """Available machine learning algorithms for DGA detection"""

    RANDOM_FOREST_CLASSIFIER = "rf"
    XG_BOOST_CLASSIFIER = "xg"
    GBM_CLASSIFIER = "gbm"


class DetectorTraining:
    """Orchestrates end-to-end training of DGA detection models.

    Manages dataset loading, model selection, training pipeline execution,
    and model persistence for domain generation algorithm detection. Supports
    multiple datasets, model types, and handles checksum-based model versioning.
    """

    def __init__(
        self,
        model_name: ModelEnum.RANDOM_FOREST_CLASSIFIER,
        model_output_path: str = f"./{RESULT_FOLDER}/model",
        dataset: DatasetEnum = DatasetEnum.COMBINE,
        data_base_path: str = "./data",
        max_rows: int = -1,
    ) -> None:
        """Initializes training configuration and dataset loading.

        Sets up model training pipeline with specified algorithm, datasets, and
        output paths. Handles existing model detection and checksum validation
        for incremental training workflows.

        Args:
            model_name (ModelEnum): ML algorithm type for training.
            model_output_path (str): Directory path for saving trained models.
            dataset (DatasetEnum): Dataset configuration for training.
            data_base_path (str): Base directory containing raw datasets.
            max_rows (int): Maximum rows per dataset (default: -1 for unlimited).

        Raises:
            NotImplementedError: If specified dataset configuration is not supported.
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

    def explain(self) -> None:
        """Generates and saves interpretable explanations for the trained model.

        Extracts decision rules and model interpretations from the trained classifier
        and saves them to text files for analysis and understanding of model behavior.
        """
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

    def test(self) -> None:
        """Evaluates trained model on all datasets and generates comprehensive reports.

        Tests model performance across all loaded datasets, computes metrics including
        classification reports, FDR, and FTTAR. Saves detailed error analysis and
        misprediction information for model debugging and improvement.
        """
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

    def train(self, seed: int = SEED) -> None:
        """Executes complete model training workflow with evaluation and persistence.

        Performs hyperparameter optimization, model training, evaluation on test set,
        and generates comprehensive analysis including model interpretation and
        performance reports across all datasets.

        Args:
            seed (int): Random seed for reproducible training results.
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
        """Calculates False Positive to True Positive Ratio (FTTAR) metric.

        Computes the ratio of false positives to true positives, which is useful
        for understanding the trade-off between detecting malicious domains and
        generating false alarms in DGA detection systems.

        Args:
            y_actual (list[int]): Ground truth binary labels.
            y_pred (list[int]): Predicted binary labels.

        Returns:
            float: FTTAR ratio (0 if no true positives detected).
        """
        _, FP, _, TP = confusion_matrix(y_actual, y_pred, labels=[0, 1]).ravel()
        if (TP) == 0:
            logger.debug("WARNING: TP = 0")
            return 0
        return FP / TP

    def _fdr(self, y_actual: list[int], y_pred: list[int]) -> float:
        """Calculates False Discovery Rate (FDR) for model evaluation.

        Computes the proportion of false positives among all positive predictions,
        which indicates the reliability of positive DGA detections in the model.

        Args:
            y_actual (list[int]): Ground truth binary labels.
            y_pred (list[int]): Predicted binary labels.

        Returns:
            float: FDR value (0 if no positive predictions made).
        """
        _, FP, _, TP = confusion_matrix(y_actual, y_pred, labels=[0, 1]).ravel()
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
        """Calculates SHA256 checksum for model file integrity verification.

        Args:
            file_path (str): Path to the model file to checksum.

        Returns:
            str: SHA256 hexadecimal digest for file validation.
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
