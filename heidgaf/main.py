import logging
import os
from enum import Enum, unique

import joblib
import polars as pl
from click import Path

from heidgaf.cache import DataFrameRedisCache
from heidgaf.detectors.arima_anomaly_detector import ARIMAAnomalyDetector
from heidgaf.detectors.base_anomaly import AnomalyDetectorConfig
from heidgaf.detectors.exponential_thresholding import EMAAnomalyDetector
from heidgaf.detectors.thresholding_algorithm import \
    ThresholdingAnomalyDetector
from heidgaf.inspectors import Inspector, InspectorConfig
from heidgaf.inspectors.domain_analyzer import DomainInspector
from heidgaf.inspectors.ip_analyzer import IPInspector


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


def inspector_factory(source: str, config: InspectorConfig) -> Inspector:
    factory = {
        "IP": (IPInspector(config)),
        "Domain": (DomainInspector(config)),
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
        threshold=5,
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
        self.threshold = threshold

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

        # Filter invalid domains
        x = x.filter(pl.col("query") != "|")
        x = x.filter(pl.col("labels").list.len() > 1)

        return x

    def run(self):
        """Starts the analyzation tasks with given data input."""

        # TODO Multithreading

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
            case "ema":
                detector = EMAAnomalyDetector(config)
            case _:
                raise NotImplementedError(f"Detector not implemented!")

        # Run anaylzers to find anomalies in data
        config = InspectorConfig(
            detector, self.df_cache, self.threshold, joblib.load("model.pkl")
        )
        for inspector in ["IP", "Domain"]:
            inspector_factory(inspector, config).run(self.data)
