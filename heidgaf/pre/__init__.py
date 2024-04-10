import datetime
import logging
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import torch

from heidgaf import ReturnCode
from heidgaf.cache import DataFrameRedisCache
from heidgaf.detectors.base_anomaly import AnomalyDetector, AnomalyDetectorConfig
from heidgaf.detectors.thresholding_algorithm import ThresholdingAnomalyDetector


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

    def set_warning(self, ip: str) -> None:
        """Creates initial warning for classifiers

        Args:
            data (pl.DataFrame): _description_
            warnings (pl.DataFrame): _description_
            id (str): _description_
        """
        logging.info(f"Analyze data in depth for {ip}")

    def update_count(
        self, df: pl.DataFrame, min_date: datetime.datetime, max_date: datetime.datetime, id: str, key: str
    ) -> None:


        # Aggregate ids by timestamp in a moving window of 6h
        id_distribution = (
            df.sort("timestamp")
            .group_by_dynamic("timestamp", every="6h", closed="right", by=id)
            .agg(pl.count())
        )
        # We generate empty datetime with zero values in a time range of 6h 
        datetimes = (id_distribution.select(
            pl.datetime_range(
                pl.col("timestamp").min(),
                pl.col("timestamp").max(),
                "6h"
            ).alias("timestamp")
        ))
        ids = (id_distribution.select(
            pl.col(id).unique()
        ))
        # Cross joining all domain
        all_dates = datetimes.join(ids, how="cross")
        # Fill with null
        id_distribution = all_dates.join(id_distribution, how="left", on=[id, "timestamp"]).fill_null(0)
        # Iterate over all unique IDs
        unique_id = id_distribution.select([id]).unique()
        for row in unique_id.rows(named=True):
            # Run detector
            unique_id_distro = id_distribution.filter(pl.col(id) == row[id])
            detector_results = self.detector.run(unique_id_distro["count"])
            if np.sum(detector_results["signals"]) > self.threshold:
                self.set_warning(row[id])

            # # Check if dns_server_frequency exists in redis cache
            # if key in self.df_cache:
            #     id_distribution = self.df_cache[key].update(
            #         id_distribution, left_on=id, right_on=id, how="outer"
            #     )
            #     logging.debug(f"Redis Data: {id_distribution}")
            # # Store information in redis client
            # self.df_cache[key] = id_distribution
            
        
    @abstractmethod
    def update_threshold(threshould, tpr, fpr):
        pass
