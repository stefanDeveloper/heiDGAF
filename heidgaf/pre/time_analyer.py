import logging

import polars as pl

from heidgaf.cache import DataFrameRedisCache
from heidgaf.pre import Analyzer


class TimeAnalyzer(Analyzer):
    KEY_IP_FREQUENCY = "client_ip_frequency"

    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def run(self, data: pl.DataFrame, df_cache: DataFrameRedisCache):
        df = data.filter(pl.col("query") != "|").filter(
            pl.col("query").str.split(".").list.len() != 1
        )

        # Update count and handle warnings
        _, warning = self.update_count(
            self, df, "client_ip", self.KEY_IP_FREQUENCY, df_cache
        )
        self.set_warning(self, df, warning, "client_ip")
