import datetime
import logging
from abc import ABCMeta, abstractmethod
from typing import Any

import polars as pl

from heidgaf import ReturnCode
from heidgaf.cache import DataFrameRedisCache, StringRedisCache


class Analyzer(metaclass=ABCMeta):
    
    
    def __init__(self) -> None:
        pass

    @classmethod
    @abstractmethod
    def run(self, data: pl.DataFrame, df_cache: DataFrameRedisCache) -> None:
        pass
    
    def set_warning(self, data: pl.DataFrame, warnings: pl.DataFrame, id: str) -> None:
        warning_data = data.join(warnings, on=id, how="semi")
        logging.info(warning_data)

    def update_count(self, df: pl.DataFrame, id: str, key: str, df_cache: DataFrameRedisCache, threshold: int) -> pl.DataFrame:
        # Dividing highest and lowest timestamp to get time range.
        # By this, we can work on relative values and are able to compare new data
        timestamp_range = (df["timestamp"].max() - df["timestamp"].min()).seconds // 3600
        if timestamp_range == 0:
            timestamp_range = 1
            
        frequency = df.group_by(id).count().with_columns(
            [
                pl.lit(datetime.datetime.now()).alias("timestamp"),
                pl.col("count").truediv(timestamp_range),
                pl.lit(timestamp_range).alias("duration")
            ]
        )
        
        # Check if dns_server_frequency exists in redis cache
        if  key in df_cache:
            frequency = df_cache[key].join(frequency, on=id, how="left")
            
            frequency = frequency.with_columns(
                    [
                        (pl.col("count").add(pl.col("count_right").fill_null(0))),
                        (
                        
                            pl.when(pl.col("timestamp_right") > pl.col("timestamp"))
                                .then(
                                    pl.col("timestamp_right")
                                ).otherwise(
                                    pl.col("timestamp")
                                )
                        ).alias("timestamp_new")
                    ]
                    
            )
            frequency = frequency.drop("timestamp", "count_right", "timestamp_right", "duration_right")
            frequency = frequency.rename(
                {
                    "timestamp_new" : "timestamp"
                }
            )
            logging.debug(f'Redis Data: {frequency}')

        ip_warnings = frequency.filter(pl.col("count") > (threshold / timestamp_range)).sort("count")
        
        # Store information in redis client
        df_cache[key] = frequency
        
        return frequency, ip_warnings

    @abstractmethod
    def update_threshold(threshould, tpr, fpr):
        pass
