
import os
import polars as pl
import redis
import logging
from enum import Enum
from click import Path

from heidgaf.cache import DataFrameRedisCache
from heidgaf.pre.ip_analyzer import IPAnalyzer


class FileType(Enum):
    CSV = "csv"
    TXT = "txt"


class Separator(Enum):
    SPACE = " "
    COMMA = ","


class DNSAnalyzerPipeline:
    def __init__(self, path: Path, redis_host="localhost", redis_port=6379, redis_db=0, redis_max_connections=20, filetype=FileType.TXT, separator=Separator.SPACE) -> None:
        self.redis_cache = DataFrameRedisCache(redis_host, redis_port, redis_db, redis_max_connections)
        
        if os.path.isfile(path):
            logging.debug(f"Processing files: {path}")
            self.data = self.load_data(path, separator.value)
        elif os.path.isdir(path):
            # TODO Handle large files, Currently redis cannot store more than 512 MB
            logging.debug(f"Processing files: {path}/*.{filetype.value}")
            self.data = self.load_data(f'{path}/*.{filetype.value}', separator.value)
        
        self.redis_cache["data"] = self.data

    def load_data(self, path, separator):
        dataframes = pl.read_csv(path, separator=separator, try_parse_dates=False,  has_header=False).with_columns(
            [
                (pl.col('column_1').str.strptime(pl.Datetime).cast(pl.Datetime)),
            ]
        )
        
        dataframes = dataframes.rename(
            {
                "column_1": "timestamp", 
                "column_2": "return_code", 
                "column_3": "client_ip", 
                "column_4": "dns_server", 
                "column_5": "query", 
                "column_6": "type", 
                "column_7": "answer", 
                "column_8": "size"
            }
        )

        return dataframes

    def run(self):
        # Running modules to analyze log files
        
        IPAnalyzer.run(self.data, self.redis_cache)
