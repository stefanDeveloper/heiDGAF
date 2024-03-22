import polars as pl
import logging

from heidgaf import ReturnCode
from heidgaf.pre import Analyzer


class IPAnalyzer(Analyzer):
    def __init__(self) -> None:
        super().__init__()
    
    @classmethod
    def run(self, data):
        # Filter data with no errors
        df = data.filter(pl.col("query") != "|").filter(pl.col("return_code") != ReturnCode.NOERROR.value).filter(pl.col("query").str.split(".").list.len() != 1)
        # Get frequency count of distinct IP addresses and DNS servers
        client_ip_frequency = df.select([
            pl.col("client_ip").value_counts()
        ])
        logging.debug(f'Client IP freq: {client_ip_frequency}')
        dns_server_frequency = df.select([
            pl.col("dns_server").value_counts()
        ])
        logging.debug(f'Client IP freq: {dns_server_frequency}')