from abc import ABCMeta, abstractmethod

import polars as pl

from heidgaf import ReturnCode
from heidgaf.cache import DataFrameRedisCache


class Analyzer(metaclass=ABCMeta):
    def __init__(self) -> None:
        pass

    @classmethod
    @abstractmethod
    def run(self, data: pl.DataFrame, redis_cache: DataFrameRedisCache):
        # Filter data with no errors
        df = data.filter(pl.col("query") != "|").filter(pl.col("return_code") != ReturnCode.NOERROR.value).filter(pl.col("query").str.split(".").list.len() != 1)