import logging
import os  # needed for Terminal execution
import re
import socket
import sys  # needed for Terminal execution

from pipeline_prototype.heidgaf_log_collector.utils import validate_host

sys.path.append(os.getcwd())  # needed for Terminal execution
from pipeline_prototype.heidgaf_log_collector import utils
from pipeline_prototype.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# LOG FORMAT:
# TIMESTAMP STATUS CLIENT_IP DNS_IP HOST_DOMAIN_NAME RECORD_TYPE RESPONSE_IP SIZE
# EXAMPLE:
# 2024-05-21T08:31:28.119Z NOERROR 192.168.0.105 8.8.8.8 www.heidelberg-botanik.de A
# b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1 150b

valid_statuses = [
    "NOERROR",
    "NXDOMAIN",
]

valid_record_types = [
    "AAAA",
    "A",
]


class LogCollector:
    server_host = None
    server_port = None
    logline = None
    timestamp = None
    status = None
    client_ip = None
    dns_ip = None
    host_domain_name = None
    record_type = None
    response_ip = None
    size = None

    def __init__(self, server_host, server_port):
        self.server_host = utils.validate_host(server_host)
        self.server_port = utils.validate_port(server_port)

    def fetch_logline(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.client_socket:
            self.client_socket.connect((str(self.server_host), self.server_port))
            while True:
                data = self.client_socket.recv(1024)
                if not data:
                    break
                self.logline = data.decode('utf-8')
                logger.info(f"Received logline: {self.logline}")

    def validate_and_extract_logline(self):
        parts = self.logline.split()

        try:
            if not self.check_length(parts):
                raise ValueError(f"Logline does not contain exactly 8 values, but {len(parts)} values were found.")
            if not self.check_timestamp(parts[0]):
                raise ValueError(f"Invalid timestamp")
            if not self.check_status(parts[1]):
                raise ValueError(f"Invalid status")
            if not self.check_domain_name(parts[4]):
                raise ValueError(f"Invalid domain name")
            if not self.check_record_type(parts[5]):
                raise ValueError(f"Invalid record type")
            if not self.check_size(parts[7]):
                raise ValueError(f"Invalid size value")
        except ValueError as e:
            raise ValueError(f"Incorrect logline: {e}")

        try:
            self.client_ip = validate_host(parts[2])
            self.dns_ip = validate_host(parts[3])
            self.response_ip = validate_host(parts[6])
        except ValueError as e:
            self.client_ip, self.dns_ip, self.response_ip = None, None, None
            raise ValueError(f"Incorrect logline: {e}")

        self.timestamp = parts[0]
        self.status = parts[1]
        self.host_domain_name = parts[4]
        self.record_type = parts[5]
        self.size = parts[7]

    @staticmethod
    def check_length(parts: list[str]) -> bool:
        return len(parts) == 8

    @staticmethod
    def check_timestamp(timestamp: str) -> bool:
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
        if re.match(pattern, timestamp):
            return True
        return False

    @staticmethod
    def check_status(status: str):
        return status in valid_statuses

    @staticmethod
    def check_domain_name(domain_name: str):
        pattern = r"^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$"
        if re.match(pattern, domain_name):
            return True
        return False

    @staticmethod
    def check_record_type(record_type: str):
        return record_type in valid_record_types

    @staticmethod
    def check_size(size: str):
        pattern = r"^\d+b$"

        if re.match(pattern, size):
            return True
        return False


if __name__ == '__main__':
    collector = LogCollector("127.0.0.1", 9998)
    collector.fetch_logline()
    collector.validate_and_extract_logline()
    print(collector.status)
