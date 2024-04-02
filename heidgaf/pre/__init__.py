from abc import ABCMeta, abstractmethod
from typing import Any

import polars as pl

from heidgaf import ReturnCode
from heidgaf.cache import DataFrameRedisCache


class Analyzer(metaclass=ABCMeta):
    def __init__(self) -> None:
        pass

    @classmethod
    @abstractmethod
    def run(self, data: pl.DataFrame, redis_cache: DataFrameRedisCache):
        pass
    
    @classmethod
    @abstractmethod
    def set_warning(self, data: Any, redis_cache: DataFrameRedisCache):
        pass