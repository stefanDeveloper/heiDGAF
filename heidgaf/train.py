import logging
from enum import Enum

import numpy as np
import torch
from fe_polars.encoding.target_encoding import TargetEncoder
from fe_polars.imputing.base_imputing import Imputer

from heidgaf import dataset
from heidgaf.cache import DataFrameRedisCache
from heidgaf.models import Pipeline
from heidgaf.models.lr import LogisticRegression
from heidgaf.post.feature import Preprocessor
from heidgaf.dataset import Dataset


class DNSAnalyzerTraining:
    def __init__(self, model: torch.nn.Module, dataset: Dataset) -> None:
        """Trainer class to fit models on data sets.

        Args:
            model (torch.nn.Module): Fit model.
            dataset (heidgaf.dataset.Dataset): Data set for training.
        """

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

        # Training model
        model_pipeline = Pipeline(
            preprocessor=Preprocessor(features_to_drop=["query"]),
            mean_imputer=Imputer(
                features_to_impute=["FQDN_full_count"], strategy="mean"
            ),
            target_encoder=TargetEncoder(smoothing=100, features_to_encode=[]),
            clf=LogisticRegression(input_dim=9, output_dim=1, epochs=5000),
        )

        model_pipeline.fit(
            x_train=dataset.dgta_dataset.X_train, y_train=dataset.dgta_dataset.Y_train
        )
