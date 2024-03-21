
import os
import polars as pl
import fnmatch
import logging
from enum import Enum
from click import Path


class FileType(Enum):
    CSV = "csv"
    TXT = "txt"


class Separator(Enum):
    SPACE = " "
    COMMA = ","


class DNSAnalyzerPipeline:
    def __init__(self, path: Path, redis_host="redis", filetype=FileType.TXT, separator=Separator.SPACE) -> None:
        if os.path.isfile(path):
            logging.debug(f"Processing files: {path}")
            self.data = self.load_data(path)
        elif os.path.isdir(path):
            logging.debug(f"Processing files: {path}/*.{filetype.value}")
            self.data = self.load_data(f'{path}/*.{filetype.value}', separator.value)

    def load_data(self, path, separator):
        dataframes = pl.read_csv(path, separator=separator, try_parse_dates=True,  has_header=False).with_columns(
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

                # (pl.col("domain").map_batches(lambda x: FQDN_entropy(x)).alias("FQDN_entropy")), # TODO: Implement Shannon Entropy
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

        return dataframes

    def run(self):
        pass
