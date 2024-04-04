from dataclasses import dataclass
import datetime
import logging
from abc import ABCMeta, abstractmethod
from typing import Any
from statsforecast import StatsForecast
import polars as pl

from heidgaf import ReturnCode
from heidgaf.cache import DataFrameRedisCache, StringRedisCache
from heidgaf.pre.anomaly import AnomalyDetectorConfig, PolarsAnomalyDetector

@dataclass
class AnalyzerConfig():
    anomDetector: PolarsAnomalyDetector

class Analyzer(metaclass=ABCMeta):
    
    
    def __init__(self) -> None:


    @classmethod
    @abstractmethod
    def run(self, data: pl.DataFrame, df_cache: DataFrameRedisCache) -> None:
        pass
    
    def set_warning(self, data: pl.DataFrame, warnings: pl.DataFrame, id: str) -> None:
        warning_data = data.join(warnings, on=id, how="semi")
        logging.info(warning_data)

    def update_count(self, df: pl.DataFrame, id: str, key: str, df_cache: DataFrameRedisCache, threshold: int, timestamp_range: int) -> pl.DataFrame:           
        df.filter(pl.col(id) == "129.206.7.163").drop("labels").write_csv("example.csv")
        id_distribution = df.sort("timestamp").group_by_dynamic("timestamp", every="6h", closed="right", by=id).agg(pl.count())

        # datetimes = (id_distribution.select(
        #     pl.datetime_range(
        #         pl.col("timestamp").min(),
        #         pl.col("timestamp").max(),
        #         "1m"
        #     ).alias("timestamp")
        # ))
        # ids = (id_distribution.select(
        #     pl.col(id).unique()
        # ))
        # all_dates = datetimes.join(ids, how="cross")
        
        # id_distribution = all_dates.join(id_distribution, how="left", on=[id, "timestamp"]).fill_null(0)
        LAG = 15
        N_STANDARD_DEVIATIONS = 3
        self.config = AnomalyDetectorConfig(lag=LAG, threshold=N_STANDARD_DEVIATIONS)
        self.anomDetector = PolarsAnomalyDetector(config)
        
        id_distribution = id_distribution.filter(pl.col(id) == "129.206.7.163").pipe(anomDetector.run, column="count", mode="optimize")
        # id_distribution = id_distribution.sort(id, "timestamp").with_columns(
        #     [
        #         (pl.col("count").rolling_mean(window_size="1h", by="timestamp", closed="right").over(id, pl.col("timestamp").dt.hour())).alias("avg_request"),
        #         (pl.col("count").rolling_std(window_size="1h", by="timestamp", closed="right").over(id, pl.col("timestamp").dt.hour())).alias("std_request")
        #     ]
        # )
        # with pl.Config(tbl_rows=5000):
            # print(id_distribution.filter(pl.col("avg_request") != 0).filter(pl.col(id) == "129.206.7.163").filter(pl.col("avg_request") > pl.col("std_request").mul(1.5)).sort("avg_request"))
            # print(id_distribution)
        # fig = PolarsAnomalyDetector(config).plot(id_distribution, "count")
        # fig.savefig("test.png")


        # id_distribution = id_distribution.rename({
        #     "timestamp": "ds",
        #     id: "unique_id",
        #     "count": "y"
        # })
        # StatsForecast.plot(id_distribution)
        # print(id_distribution.group_by(id).agg(pl.col("avg_request").median()).sort("avg_request"))
        # frequency = df.group_by_dynamic("timestamp", every="1d", closed="right").agg(pl.col(id)).sum().with_columns(
        #     [
        #         pl.lit(datetime.datetime.now()).alias("timestamp"),
        #         pl.col("count").truediv(timestamp_range),
        #         pl.lit(timestamp_range).alias("duration")
        #     ]
        # )
        
        # # Check if dns_server_frequency exists in redis cache
        if  key in df_cache:
            id_distribution = df_cache[key].update(id_distribution, left_on=id, right_on=id, how="outer")
            
        #     frequency = frequency.with_columns(
        #             [
        #                 (pl.col("count").add(pl.col("count_right").fill_null(0))),
        #                 (
                        
        #                     pl.when(pl.col("timestamp_right") > pl.col("timestamp"))
        #                         .then(
        #                             pl.col("timestamp_right")
        #                         ).otherwise(
        #                             pl.col("timestamp")
        #                         )
        #                 ).alias("timestamp_new")
        #             ]
                    
        #     )
        #     frequency = frequency.drop("timestamp", "count_right", "timestamp_right", "duration_right")
        #     frequency = frequency.rename(
        #         {
        #             "timestamp_new" : "timestamp"
        #         }
        #     )
            logging.debug(f'Redis Data: {id_distribution}')

        ip_warnings = id_distribution.filter(pl.col("count") > (threshold / timestamp_range)).sort("count")
        
        # Store information in redis client
        df_cache[key] = id_distribution
        
        return id_distribution, ip_warnings

    @abstractmethod
    def update_threshold(threshould, tpr, fpr):
        pass
