import datetime
import logging
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, List
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import torch
from fe_polars.encoding.target_encoding import TargetEncoder
from fe_polars.imputing.base_imputing import Imputer
from heidgaf import ReturnCode
from heidgaf.cache import DataFrameRedisCache
from heidgaf.detectors.base_anomaly import AnomalyDetector, AnomalyDetectorConfig
from heidgaf.detectors.thresholding_algorithm import ThresholdingAnomalyDetector
from heidgaf.models import Pipeline
from heidgaf.post.feature import Preprocessor


@dataclass
class AnalyzerConfig:
    """Configuration class of Analyzers"""

    detector: AnomalyDetector
    df_cache: DataFrameRedisCache
    threshold: 3
    model: torch.nn.Module


class Analyzer(metaclass=ABCMeta):
    """_summary_

    Args:
        metaclass (_type_, optional): _description_. Defaults to ABCMeta.
    """

    def __init__(self, config: AnalyzerConfig) -> None:
        self.detector = config.detector
        self.df_cache = config.df_cache
        self.threshold = config.threshold
        self.model = config.model

    @abstractmethod
    def run(self, data: pl.DataFrame) -> None:
        """_summary_

        Args:
            data (pl.DataFrame): _description_
            df_cache (DataFrameRedisCache): _description_
        """
        pass

    def set_warning(self, data: pl.DataFrame, warnings: List, id: str) -> None:
        """Creates initial warning for classifiers

        Args:
            data (pl.DataFrame): _description_
            warnings (pl.DataFrame): _description_
            id (str): _description_
        """
        model_pipeline = Pipeline(
            preprocessor=Preprocessor(
                features_to_drop=[
                    "timestamp",
                    "return_code",
                    "client_ip",
                    "dns_server",
                    "query",
                    "type",
                    "answer",
                    "size",
                    "query",
                    "labels",
                    "thirdleveldomain",
                    "secondleveldomain",
                    "fqdn",
                ]
            ),
            mean_imputer=Imputer(features_to_impute=[], strategy="mean"),
            target_encoder=TargetEncoder(smoothing=100, features_to_encode=[]),
            clf=self.model,
        )
        for warning in warnings:
            logging.debug(f"Analyze data in depth for {warning}")
            
            data_id = data.filter(pl.col(id) == warning)

            # Predict data based on model
            y_pred = model_pipeline.predict(data_id)
            
            indices = np.where(y_pred == 1)[0]
            data_id = data_id.with_row_count(name="idx", offset=0)
            supicious_data = data_id.filter(pl.col("idx").is_in(indices))
            
            
            if not supicious_data.is_empty():
                print(f"{warning} has following errors")
                print(supicious_data.select(["fqdn"]).unique())

    def update_count(
        self,
        df: pl.DataFrame,
        min_date: datetime.datetime,
        max_date: datetime.datetime,
        id: str,
        key: str,
    ) -> None:
        warnings = []

        # Aggregate ids by timestamp in a moving window of 6h
        id_distribution = (
            df.sort("timestamp")
            .group_by_dynamic("timestamp", every="6h", closed="right", by=id)
            .agg(pl.count())
        )
        # We generate empty datetime with zero values in a time range of 6h
        datetimes = id_distribution.select(
            pl.datetime_range(
                min_date.replace(microsecond=0), max_date.replace(microsecond=0), "6h"
            ).alias("timestamp")
        )
        ids = id_distribution.select(pl.col(id).unique())
        # Cross joining all domain
        all_dates = datetimes.join(ids, how="cross")
        # Fill with null
        id_distribution = all_dates.join(
            id_distribution, how="left", on=[id, "timestamp"]
        ).fill_null(0)
        
        # Iterate over all unique IDs
        unique_id = id_distribution.select([id]).unique()
        for row in unique_id.rows(named=True):
            # Run detector
            unique_id_distro = id_distribution.filter(pl.col(id) == row[id])
            detector_results = self.detector.run(unique_id_distro["count"])
            
            # If total count of signals is higher than threshold, we append our supsicious IPs to our warnings list
            if np.sum(detector_results["signals"]) > self.threshold:
                warnings.append(row[id])

            # TODO Update cache
            # # Check if dns_server_frequency exists in redis cache
            # if key in self.df_cache:
            #     id_distribution = self.df_cache[key].update(
            #         id_distribution, left_on=id, right_on=id, how="outer"
            #     )
            #     logging.debug(f"Redis Data: {id_distribution}")
            # # Store information in redis client
            # self.df_cache[key] = id_distribution

        return warnings

    @abstractmethod
    def update_threshold(threshould, tpr, fpr):
        pass
