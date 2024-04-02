import polars as pl
import redis

from heidgaf import ReturnCode
from heidgaf.cache import DataFrameRedisCache
from heidgaf.pre import Analyzer


class DomainAnalyzer(Analyzer):
    def __init__(self) -> None:
        super().__init__()
    
    @classmethod
    def run(self, data: pl.DataFrame, redis_cache: DataFrameRedisCache):
        # Filter data with no errors
        df = data.filter(pl.col("query") != "|").filter(pl.col("return_code") != ReturnCode.NOERROR.value).filter(pl.col("query").str.split(".").list.len() != 1)
        
    