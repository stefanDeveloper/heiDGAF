import logging

import polars as pl

from heidgaf import ReturnCode
from heidgaf.cache import DataFrameRedisCache
from heidgaf.pre import Analyzer


class IPAnalyzer(Analyzer):
    KEY_IP_FREQUENCY = "client_ip_frequency"
    KEY_DNS_SERVER = "dns_server_frequency"
    KEY_SLD = "sld_frequency"
    
    def __init__(self) -> None:
        super().__init__()
        self.threshold = 200
    
    @classmethod
    def run(self, data: pl.DataFrame, redis_cache: DataFrameRedisCache):           
        # Filter data with no errors
        df = data.filter(pl.col("query") != "|").filter(pl.col("return_code") != ReturnCode.NOERROR.value).filter(pl.col("query").str.split(".").list.len() != 1)
        
        # Update IP frequency based on errors
        self.__update_count(df, "client_ip", self.KEY_IP_FREQUENCY, redis_cache)
        self.__update_count(df, "dns_server", self.KEY_DNS_SERVER, redis_cache)
        
        self.__update_count(df, "SLD", self.KEY_SLD, redis_cache)
        
        # TODO: Process frequency and return values
        # TODO: Check if IP has more than threshold error request -> if yes, check distribution.
        
        
    def __update_count(df: pl.DataFrame, id: str, key: str, redis_cache: DataFrameRedisCache) -> None:
        frequency = df.group_by(id).count()
        
        # Check if dns_server_frequency exists in redis cache
        if  key in redis_cache:
            frequency = pl.concat([redis_cache[key], frequency]).groupby(id).agg(pl.sum('count'))
            logging.debug(f'Redis Data: {frequency}')
            
        # Store information in redis client
        redis_cache[key] = frequency
        
    