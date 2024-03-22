
import os
import polars as pl
import redis
import logging
from enum import Enum
from click import Path

from heidgaf.pre.ip_analyzer import IPAnalyzer


class FileType(Enum):
    CSV = "csv"
    TXT = "txt"


class Separator(Enum):
    SPACE = " "
    COMMA = ","


class DNSAnalyzerPipeline:
    def __init__(self, path: Path, redis_host="localhost", redis_port=6379, redis_db=0, redis_max_connections=20, filetype=FileType.TXT, separator=Separator.SPACE) -> None:
        logging.debug("Connect to Redis server")
        pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=redis_db, max_connections=redis_max_connections)
        self.redis_client = redis.Redis(connection_pool=pool)
        self.redis_client.ping() 

        
        if os.path.isfile(path):
            logging.debug(f"Processing files: {path}")
            self.data = self.load_data(path, separator.value)
        elif os.path.isdir(path):
            logging.debug(f"Processing files: {path}/*.{filetype.value}")
            self.data = self.load_data(f'{path}/*.{filetype.value}', separator.value)
        
        self.redis_client.set("data", self.data.write_ipc(file = None, compression="lz4").getvalue())

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
        
        dataframes = dataframes.with_columns(
            [
                (pl.col("query").str.split(".").alias("labels")),
                (pl.col("query").str.split(".").list.len().alias("label_length")),
                (pl.col("query").str.split(".").list.max().str.len_chars().alias("label_max")),
                (pl.col("query").str.strip_chars(".").str.len_chars().alias("label_average")),
            ]
        )
        
        dataframes = dataframes.with_columns(
            [   

                # FQDN
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(-2)
                    ).otherwise(
                        pl.col("labels").list.get(0)
                    ).alias("SLD")),
                (pl.col("query").str.len_chars().alias("FQDN_full_count")),
                (pl.col("query").str.count_matches(r"[0-9]").alias("FQDN_numeric_count")),
                (pl.col("query").str.count_matches(r"[^\w\s]").alias("FQDN_special_count")),
                # Third-level domain
                (pl.when(pl.col("labels").list.len() > 2).then(pl.col("labels").list.get(0).str.len_chars()).otherwise(0).alias("SD_full_count")),
                (pl.when(pl.col("labels").list.len() > 2).then(pl.col("labels").list.get(0).str.count_matches(r"[0-9]")).otherwise(0).alias("SD_numeric_count")),
                (pl.when(pl.col("labels").list.len() > 2).then(pl.col("labels").list.get(0).str.count_matches(r"[^\w\s]")).otherwise(0).alias("SD_special_count")),
                # Second-level domain
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(1).str.len_chars()
                    ).otherwise(
                        pl.col("labels").list.get(0).str.len_chars()
                    ).alias("SLD_full_count")),
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(1).str.count_matches(r"[0-9]")
                    ).otherwise(
                        pl.col("labels").list.get(0).str.count_matches(r"[0-9]")
                    ).alias("SLD_numeric_count")),
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(1).str.count_matches(r"[^\w\s]")
                    ).otherwise(
                        pl.col("labels").list.get(0).str.count_matches(r"[^\w\s]")
                    ).alias("SLD_special_count")),
            ]
        )
        
        dataframes = dataframes.with_columns([
            (pl.col("query").entropy(base=2).alias("FQDN_entropy")),
        ])

        return dataframes

    def run(self):
        IPAnalyzer.run(self.data, self.redis_client)
