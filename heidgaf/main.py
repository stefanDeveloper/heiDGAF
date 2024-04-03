
import logging
import os
from enum import Enum

import polars as pl
from click import Path

from heidgaf.cache import DataFrameRedisCache, StringRedisCache
from heidgaf.post.feature import Preprocessor
from heidgaf.pre.domain_analyzer import DomainAnalyzer
from heidgaf.pre.ip_analyzer import IPAnalyzer
from heidgaf.pre.time_analyer import TimeAnalyzer


class FileType(Enum):
    CSV = "csv"
    TXT = "txt"


class Separator(Enum):
    SPACE = " "
    COMMA = ","


class DNSAnalyzerPipeline:
    def __init__(self, path: Path, redis_host="localhost", redis_port=6379, redis_db=0, redis_max_connections=20, filetype=FileType.TXT, separator=Separator.SPACE) -> None:
        self.df_cache = DataFrameRedisCache(redis_host, redis_port, redis_db, redis_max_connections)
        self.string_cache = StringRedisCache(redis_host, redis_port, redis_db, redis_max_connections)
        
        if os.path.isfile(path):
            logging.debug(f"Processing files: {path}")
            self.data = self.load_data(path, separator.value)
        elif os.path.isdir(path):
            logging.debug(f"Processing files: {path}/*.{filetype.value}")
            self.data = self.load_data(f'{path}/*.{filetype.value}', separator.value)
        
    def load_data(self, path, separator):
        x = pl.read_csv(path, separator=separator, try_parse_dates=False,  has_header=False).with_columns(
            [
                (pl.col('column_1').str.strptime(pl.Datetime).cast(pl.Datetime))
            ]
        )
        
        x = x.rename(
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

        x = x.with_columns(
            [
                (pl.col("query").str.split(".").alias("labels")),
            ]
        )
        
        x = x.with_columns(
            [   
                # FQDN
                (pl.col("query")).alias("fqdn"),
            ]
        )
        
        x = x.with_columns(
            [   
                # Second-level domain
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(-2)
                    ).otherwise(
                        pl.col("labels").list.get(0)
                    ).alias("secondleveldomain"))
            ]
        )
        
        x = x.with_columns(
            [   
                # Third-level domain
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                    pl.col("labels").list.slice(0, pl.col("labels").list.len() - 2).list.join(".")
                    ).otherwise(pl.lit("")).alias("thirdleveldomain")),
            ]
        )

        return x

    def run(self):
        
        # Running modules to analyze log files
        # TODO Multithreading
        # TODO Handle warnings for machine learning predictions
        # preprocessor = Preprocessor(features_to_drop=[])
        # processed_data = preprocessor.transform(self.data)
        
        IPAnalyzer.run(self.data, self.df_cache)
        # DomainAnalyzer.run(self.data, self.df_cache)
        # TimeAnalyzer.run(self.data, self.df_cache)
