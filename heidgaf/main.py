from dataclasses import dataclass
import logging
import os
from enum import Enum, unique

import polars as pl
from click import Path

from heidgaf.cache import DataFrameRedisCache, StringRedisCache
from heidgaf.detectors.arima_anomaly_detector import ARIMAAnomalyDetector
from heidgaf.detectors.base_anomaly import AnomalyDetectorConfig
from heidgaf.detectors.exponential_thresholding import EMAAnomalyDetector
from heidgaf.detectors.thresholding_algorithm import ThresholdingAnomalyDetector
from heidgaf.post.feature import Preprocessor
from heidgaf.pre import Analyzer, AnalyzerConfig
from heidgaf.pre.domain_analyzer import DomainAnalyzer
from heidgaf.pre.ip_analyzer import IPAnalyzer
from heidgaf.pre.time_analyer import TimeAnalyzer


@unique
class Detector(str, Enum):
    THRESHOLDING = "threshold"
    EMA = "ema"
    ARIMA = "arima"


@unique
class FileType(Enum):
    CSV = "csv"
    TXT = "txt"


@unique
class Separator(Enum):
    SPACE = " "
    COMMA = ","


def analyzer_factory(source: str, config: AnalyzerConfig) -> Analyzer:
    factory = {
        "IP": (IPAnalyzer(config)),
        "Domain": (DomainAnalyzer(config)),
        "Time": (TimeAnalyzer(config)),
    }
    if source in factory:
        return factory[source]
    else:
        raise ValueError(
            f"source {source} is not supported. Please pass a valid source."
        )


class DNSAnalyzerPipeline:
    """Main analyzer pipeline. It loads new data and processes it through our analyzers. If an anomaly occurs, our models run"""

    def __init__(
        self,
        path: Path,
        lag: float = 15,
        n_standard_deviations: float = 3,
        anomaly_influence: float = 0.7,
        filetype=FileType.TXT,
        separator=Separator.SPACE,
        detector=Detector.THRESHOLDING,
        redis_host="localhost",
        redis_port=6379,
        redis_db=0,
        redis_max_connections=20,
        threshold=3
    ) -> None:
        self.df_cache = DataFrameRedisCache(
            redis_host, redis_port, redis_db, redis_max_connections
        )

        if os.path.isfile(path):
            logging.debug(f"Processing files: {path}")
            self.data = self.load_data(path, separator.value)
        elif os.path.isdir(path):
            logging.debug(f"Processing files: {path}/*.{filetype.value}")
            self.data = self.load_data(f"{path}/*.{filetype.value}", separator.value)
        else:
            FileNotFoundError(f"File or path not found: {path}")

        self.lag = lag
        self.n_standard_deviations = n_standard_deviations
        self.anomaly_influence = anomaly_influence
        self.detector = detector
        self.threshold = 3

    def load_data(self, path: str, separator: str) -> pl.DataFrame:
        """Loads data from csv files

        Args:
            path (str): Path to files
            separator (str): separator of colums

        Returns:
            pl.DataFrame: Polars Dataframe
        """
        x = pl.read_csv(
            path, separator=separator, try_parse_dates=False, has_header=False
        ).with_columns(
            [(pl.col("column_1").str.strptime(pl.Datetime).cast(pl.Datetime))]
        )

        x = x.rename(
            {
                "column_1": "timestamp",
                "column_2": "return_code",
                "column_3": "client_ip",
                "column_4": "dns_server",
                "column_5": "query",
                "column_6": "type",
                "column_7": "answer",
                "column_8": "size",
            }
        )

        x = x.with_columns(
            [
                (pl.col("query").str.split(".").alias("labels")),
            ]
        )

        x = x.with_columns(
            [
                # FQDN
                (pl.col("query")).alias("fqdn"),
            ]
        )

        x = x.with_columns(
            [
                # Second-level domain
                (
                    pl.when(pl.col("labels").list.len() > 2)
                    .then(pl.col("labels").list.get(-2))
                    .otherwise(pl.col("labels").list.get(0))
                    .alias("secondleveldomain")
                )
            ]
        )

        x = x.with_columns(
            [
                # Third-level domain
                (
                    pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels")
                        .list.slice(0, pl.col("labels").list.len() - 2)
                        .list.join(".")
                    )
                    .otherwise(pl.lit(""))
                    .alias("thirdleveldomain")
                ),
            ]
        )

        return x

    def run(self):
        """Starts the analyzation tasks with given data input."""

        # TODO Multithreading
        # TODO Handle warnings for machine learning predictions

        # preprocessor = Preprocessor(features_to_drop=[])
        # processed_data = preprocessor.transform(self.data)

        # Creates anomaly detector
        config = AnomalyDetectorConfig(
            self.lag, self.n_standard_deviations, self.anomaly_influence
        )
        detector = None
        match self.detector:
            case "threshold":
                detector = ThresholdingAnomalyDetector(config)
            case "arima":
                detector = ARIMAAnomalyDetector(config)
            case "threshold":
                detector = EMAAnomalyDetector(config)
            case _:
                raise NotImplementedError(f"Detector not implemented!")

        # Run anaylzers to find anomalies in data
        config = AnalyzerConfig(detector, self.df_cache, self.threshold)
        for analyzer in ["IP"]:
            analyzer_factory(analyzer, config).run(self.data)
