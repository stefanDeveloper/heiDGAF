import datetime
import logging
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any
import matplotlib.pyplot as plt
import polars as pl
from statsforecast import StatsForecast

from heidgaf import ReturnCode
from heidgaf.cache import DataFrameRedisCache, StringRedisCache
from heidgaf.detectors.base_anomaly import AnomalyDetector, AnomalyDetectorConfig
from heidgaf.detectors.thresholding_algorithm import ThresholdingAnomalyDetector


@dataclass
class AnalyzerConfig:
    """Configuration class of Analyzers"""

    detector: AnomalyDetector


class Analyzer(metaclass=ABCMeta):
    """_summary_

    Args:
        metaclass (_type_, optional): _description_. Defaults to ABCMeta.
    """

    def __init__(self, config: AnalyzerConfig) -> None:
        self.detector = config.detector

    @abstractmethod
    def run(self, data: pl.DataFrame, df_cache: DataFrameRedisCache) -> None:
        """_summary_

        Args:
            data (pl.DataFrame): _description_
            df_cache (DataFrameRedisCache): _description_
        """
        pass

    def set_warning(self, data: pl.DataFrame, warnings: pl.DataFrame, id: str) -> None:
        """Creates initial warning for classifiers

        Args:
            data (pl.DataFrame): _description_
            warnings (pl.DataFrame): _description_
            id (str): _description_
        """
        warning_data = data.join(warnings, on=id, how="semi")
        logging.info(warning_data)

    def update_count(
        self, df: pl.DataFrame, id: str, key: str, df_cache: DataFrameRedisCache
    ) -> pl.DataFrame:
        """Update count of id

        Args:
            df (pl.DataFrame): pre-filtered data
            id (str): ID of column
            key (str): Redis key
            df_cache (DataFrameRedisCache): RedisCache for storing pl.DataFrame

        Returns:
            pl.DataFrame: Current data
        """

        # Aggregate ids by timestamp
        id_distribution = (
            df.sort("timestamp")
            .group_by_dynamic("timestamp", every="6h", closed="right", by=id)
            .agg(pl.count())
        )

        # datetimes = (id_distribution.select(
        #     pl.datetime_range(
        #         pl.col("timestamp").min(),
        #         pl.col("timestamp").max(),
        #         "6h"
        #     ).alias("timestamp")
        # ))
        # ids = (id_distribution.select(
        #     pl.col(id).unique()
        # ))
        # all_dates = datetimes.join(ids, how="cross")

        # id_distribution = all_dates.join(id_distribution, how="left", on=[id, "timestamp"]).fill_null(0)

        LAG = 15
        N_STANDARD_DEVIATIONS = 3
        ANOMALY_INFLUENCE = 0.7
        config = AnomalyDetectorConfig(LAG, N_STANDARD_DEVIATIONS, ANOMALY_INFLUENCE)
        anomDetector = ThresholdingAnomalyDetector(config)
        # results = anomDetector.run(y)

        # config = AnomalyDetectorConfig(lag=LAG, threshold=N_STANDARD_DEVIATIONS)
        # anomDetector = PolarsAnomalyDetector(config)

        id_distribution = id_distribution.filter(pl.col(id) == "129.206.7.163")
        results = anomDetector.run(id_distribution["count"])
        # id_distribution = id_distribution.pipe(anomDetector.run, column="count", mode="optimize")
        # p_df.select([pl.col("signal").value_counts(sort=True)])

        # with pl.Config(tbl_rows=5000):
        # print(id_distribution)
        # fig = PolarsAnomalyDetector(config).plot(id_distribution, "count")
        fig = anomDetector.plot(id_distribution["count"].to_pandas(), results)
        fig.savefig("test_3.png")

        # # Check if dns_server_frequency exists in redis cache
        if key in df_cache:
            id_distribution = df_cache[key].update(
                id_distribution, left_on=id, right_on=id, how="outer"
            )
            logging.debug(f"Redis Data: {id_distribution}")

        # TODO Filter
        # ip_warnings = id_distribution.filter(pl.col("count") > (threshold / timestamp_range)).sort("count")

        # Store information in redis client
        df_cache[key] = id_distribution

        return id_distribution, None

    @abstractmethod
    def update_threshold(threshould, tpr, fpr):
        pass
