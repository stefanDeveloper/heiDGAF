
from heidgaf.cache import DataFrameRedisCache
from heidgaf.pre import Analyzer


class TimeAnalyzer(Analyzer):
    def __init__(self) -> None:
        super().__init__()
    
    @classmethod
    def run(self, data: pl.DataFrame, redis_cache: DataFrameRedisCache):
        pass