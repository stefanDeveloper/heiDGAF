import logging
from typing import Any

import polars as pl
import redis


class DataFrameRedisCache(object):
    def __init__(self ,redis_host="localhost", redis_port=6379, redis_db=0, redis_max_connections=20,) -> None:
        logging.debug("Connect to Redis server")
        self.pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=redis_db, max_connections=redis_max_connections)
        self.redis_client = redis.Redis(connection_pool=self.pool)
        self.redis_client.ping()
        
    def __getitem__(self, key: str) -> pl.DataFrame:
        if self.redis_client.exists(key):
            return pl.read_ipc(self.redis_client.get(key))
        else:
            return pl.DataFrame({})
    
    def __contains__(self, key):
        return self.redis_client.exists(key)
    
    def __str__(self):
        return f'Redis has stored following keys: {self.redis_client.keys}'

    def __setitem__(self, key: str, df: pl.DataFrame) -> pl.DataFrame:
        self.redis_client.set(key, df.write_ipc(file=None, compression="lz4").getvalue())
    
    def __delitem__(self, key: str) -> None:
        self.redis_client.delete(key)
    