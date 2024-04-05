import datetime
import logging

import numpy as np
import polars as pl

from heidgaf import ReturnCode
from heidgaf.cache import DataFrameRedisCache
from heidgaf.pre import Analyzer


class IPAnalyzer(Analyzer):
    KEY_IP_FREQUENCY = "client_ip_error_frequency"
    KEY_DNS_SERVER = "dns_server_error_frequency"

    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def run(self, data: pl.DataFrame, df_cache: DataFrameRedisCache) -> pl.DataFrame:
        # Filter data with no errors
        df = (
            data.filter(pl.col("query") != "|")
            .filter(pl.col("return_code") != ReturnCode.NOERROR.value)
            .filter(pl.col("query").str.split(".").list.len() != 1)
        )

        # Update frequencies based on errors
        _, warning = self.update_count(
            self, df, "client_ip", self.KEY_IP_FREQUENCY, df_cache
        )
        self.set_warning(self, df, warning, "client_ip")

        _, warning = self.update_count(
            self, df, "dns_server", self.KEY_DNS_SERVER, df_cache
        )
        self.set_warning(self, df, warning, "dns_server")
