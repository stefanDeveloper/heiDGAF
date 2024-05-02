import datetime
import logging
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import List

import numpy as np
import polars as pl
import torch
from fe_polars.encoding.target_encoding import TargetEncoder
from fe_polars.imputing.base_imputing import Imputer

from heidgaf.cache import DataFrameRedisCache
from heidgaf.detectors.base_anomaly import AnomalyDetector
from heidgaf.feature import Preprocessor
from heidgaf.models import Pipeline


@dataclass
class InspectorConfig:
    """Configuration class of inspectors."""

    detector: AnomalyDetector
    df_cache: DataFrameRedisCache
    threshold: 3
    model: torch.nn.Module


class Inspector(metaclass=ABCMeta):
    """Metaclass to test DNS requests.

    Args:
        metaclass (_type_, optional): Metaclass object. Defaults to ABCMeta.
    """

    def __init__(self, config: InspectorConfig) -> None:
        """Initializes detector, cache, threshold, and model.

        Args:
            config (TesterConfig): TesterConfig.
        """
        self.detector: AnomalyDetector = config.detector
        self.df_cache = config.df_cache
        self.threshold = config.threshold
        self.model = config.model

        self.model_pipeline = Pipeline(
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
                    "tld"
                ]
            ),
            mean_imputer=Imputer(features_to_impute=[], strategy="mean"),
            target_encoder=TargetEncoder(smoothing=100, features_to_encode=[]),
            clf=self.model,
        )

    @abstractmethod
    def run(self, data: pl.DataFrame) -> pl.DataFrame:
        """Runs tester.

        Args:
            data (pl.DataFrame): Preprocessed data

        Returns:
            pl.DataFrame: Suspicious Ids.
        """
        pass

    def warnings(self, data: pl.DataFrame, suspicious: List, id: str) -> pl.DataFrame:
        """Creates initial warning for classifiers

        Args:
            data (pl.DataFrame): Preprocessed data.
            suspicious (List): Suspicious Id's retrieved by detectors.
            id (str): Id of column to process.
        """

        fqdn_distro = data.group_by("fqdn").agg(
            pl.col("return_code")
            .is_in(["NXDOMAIN", "SERVFAIL"])
            .sum()
            .truediv(
                pl.col("client_ip").count().truediv(pl.col("client_ip").n_unique())
            )
            .alias("distro")
        )
        fqdn_distro = fqdn_distro.filter(pl.col("distro") > 0.05)
        
        # Initialize empty array
        total_warnings = [data.clear()]

        for warning in suspicious:
            logging.debug(f"Analyze data in depth for {warning}")

            data_id = data.filter(pl.col(id) == warning).filter(
                pl.col("fqdn").is_in(fqdn_distro["fqdn"].to_list())
            )
            suspicious_data = data.clear()
            if not data_id.is_empty():
                # Predict data based on model
                # TODO Create ensemble classification
                # TODO Set to regressor
                y_pred = self.model_pipeline.predict(data_id)

                indices = np.where(y_pred == 1)[0]
                data_id = data_id.with_row_count(name="idx", offset=0)
                suspicious_data = data_id.filter(pl.col("idx").is_in(indices))
                suspicious_data = suspicious_data.drop("idx")

                if not suspicious_data.is_empty():
                    logging.debug(f"{warning} has following errors")
                    with pl.Config(tbl_rows=100):
                        logging.debug(suspicious_data.select(["fqdn"]).unique())
                    total_warnings.append(suspicious_data)
        
        return pl.concat(total_warnings)
                    

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
            .group_by_dynamic("timestamp", every="3h", closed="right", by=id)
            .agg(pl.count())
        )
        # We generate empty datetime with zero values in a time range of 6h
        datetimes = id_distribution.select(
            pl.datetime_range(
                min_date.replace(microsecond=0), max_date.replace(microsecond=0), "3h"
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
