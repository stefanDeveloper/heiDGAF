import polars as pl
import redis

from heidgaf import ReturnCode
from heidgaf.cache import DataFrameRedisCache
from heidgaf.pre import Analyzer


class DomainAnalyzer(Analyzer):
    KEY_SECOND_LEVEL_DOMAIN = "secondleveldomain_frequency"
    KEY_THIRD_LEVEL_DOMAIN = "thirdleveldomain_frequency"
    KEY_FQDN = "fqdn_frequency"
    
    def __init__(self) -> None:
        super().__init__()
    
    @classmethod
    def run(self, data: pl.DataFrame, redis_cache: DataFrameRedisCache) -> pl.DataFrame:
        # Filter data with no errors
        df = data.filter(pl.col("query") != "|").filter(pl.col("query").str.split(".").list.len() != 1)
        
        _, warning = self.update_count(self, df, "fqdn", self.KEY_FQDN, redis_cache, 5000)
        self.set_warning(self, df, warning, "fqdn")
        
        _, warning = self.update_count(self, df, "secondleveldomain", self.KEY_SECOND_LEVEL_DOMAIN, redis_cache, 5000)
        self.set_warning(self, df, warning, "secondleveldomain")
        
        _, warning = self.update_count(self, df, "thirdleveldomain", self.KEY_THIRD_LEVEL_DOMAIN, redis_cache, 5000)
        self.set_warning(self, df, warning, "thirdleveldomain")
