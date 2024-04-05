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
from heidgaf.pre import AnalyzerConfig
from heidgaf.pre.domain_analyzer import DomainAnalyzer
from heidgaf.pre.ip_analyzer import IPAnalyzer
from heidgaf.pre.time_analyer import TimeAnalyzer


@unique
class Detector(Enum):
    THRESHOLDING = ThresholdingAnomalyDetector
    EMA = EMAAnomalyDetector
    ARIMA = ARIMAAnomalyDetector


@unique
class FileType(Enum):
    CSV = "csv"
    TXT = "txt"


@unique
class Separator(Enum):
    SPACE = " "
    COMMA = ","

@dataclass
class RedisCacheConfig:
    redis_host: str ="localhost",
    redis_port: int =6379,
    redis_db: int =0,
    redis_max_connections: int =20,

@dataclass
class DNSAnalyzerPipelineConfig:
    lag: float = 15
    n_standard_deviations: float = 3
    anomaly_influence: float = 0.7
    filetype: FileType = FileType.TXT,
    separator: FileType = Separator.SPACE,


class DNSAnalyzerPipeline:
    """Main analyzer pipeline. It loads new data and processes it through our analyzers. If an anomaly occurs, our models run"""

    def __init__(
        self,
        path: Path,
        redis_host="localhost",
        redis_port=6379,
        redis_db=0,
        redis_max_connections=20,
        filetype=FileType.TXT,
        separator=Separator.SPACE,
    ) -> None:
        self.df_cache = DataFrameRedisCache(
            redis_host, redis_port, redis_db, redis_max_connections
        )
        self.string_cache = StringRedisCache(
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
        """_summary_"""
        # Running modules to analyze log files
        # TODO Multithreading
        # TODO Handle warnings for machine learning predictions
        # preprocessor = Preprocessor(features_to_drop=[])
        # processed_data = preprocessor.transform(self.data)
        LAG = 15
        N_STANDARD_DEVIATIONS = 3
        ANOMALY_INFLUENCE = 0.7

        config = AnomalyDetectorConfig(LAG, N_STANDARD_DEVIATIONS, ANOMALY_INFLUENCE)

        thresholding_detector = ThresholdingAnomalyDetector(config)

        config = AnalyzerConfig()

        IPAnalyzer.run(self.data, self.df_cache)
        # DomainAnalyzer.run(self.data, self.df_cache)
        # TimeAnalyzer.run(self.data, self.df_cache)
