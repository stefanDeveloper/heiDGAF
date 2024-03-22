import polars as pl
import logging
import redis

from heidgaf import ReturnCode
from heidgaf.pre import Analyzer


class IPAnalyzer(Analyzer):
    IP_ANALYZER_KEY_IP = "client_ip_frequency"
    IP_ANALYZER_KEY_SERVER = "dns_server_frequency"
    
    def __init__(self) -> None:
        super().__init__()
        self.threshold = 200
    
    @classmethod
    def run(self, data: pl.DataFrame, redis_client: redis.Redis):           
        # Filter data with no errors
        df = data.filter(pl.col("query") != "|").filter(pl.col("return_code") != ReturnCode.NOERROR.value).filter(pl.col("query").str.split(".").list.len() != 1)
        # Get frequency count of distinct IP addresses and DNS servers
        client_ip_frequency = df.group_by("client_ip").count()
        logging.debug(f'Client IP freq: {client_ip_frequency}')
        
        dns_server_frequency = df.group_by("dns_server").count()
        logging.debug(f'DNS Server IP freq: {dns_server_frequency}')
        
        # Check if ip_frequency exists in redis cache
        if redis_client.exists(self.IP_ANALYZER_KEY_IP):
            redis_data = pl.read_ipc(redis_client.get(self.IP_ANALYZER_KEY_IP))
            client_ip_frequency = pl.concat([redis_data, client_ip_frequency]).groupby('client_ip').agg(pl.sum('count')).sort(by="count")
            logging.debug(f'Redis Data: {client_ip_frequency}')

        # Check if dns_server_frequency exists in redis cache
        if redis_client.exists(self.IP_ANALYZER_KEY_SERVER):
            redis_data = pl.read_ipc(redis_client.get(self.IP_ANALYZER_KEY_SERVER))
            dns_server_frequency = pl.concat([redis_data, dns_server_frequency]).groupby('dns_server').agg(pl.sum('count')).sort(by="count")
            logging.debug(f'Redis Data: {dns_server_frequency}')
            

        # Store information in redis client
        redis_client.set(self.IP_ANALYZER_KEY_IP, client_ip_frequency.write_ipc(file=None, compression="lz4").getvalue())
        redis_client.set(self.IP_ANALYZER_KEY_SERVER, dns_server_frequency.write_ipc(file=None, compression="lz4").getvalue())