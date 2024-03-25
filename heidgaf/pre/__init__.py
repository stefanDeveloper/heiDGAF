from abc import ABCMeta, abstractmethod

import polars as pl

from heidgaf.cache import DataFrameRedisCache


class Analyzer(metaclass=ABCMeta):
    def __init__(self) -> None:
        pass

    @classmethod
    @abstractmethod
    def run(self, data: pl.DataFrame, redis_cache: DataFrameRedisCache):
        pass