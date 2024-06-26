import json
import logging
import os  # needed for Terminal execution
import re
import socket
import sys

import yaml

sys.path.append(os.getcwd())  # needed for Terminal execution
from heidgaf_core.config import CONFIG_FILEPATH  # needed for Terminal execution
from heidgaf_core.batch_handler import KafkaBatchSender
from heidgaf_core.utils import validate_host
from heidgaf_core import utils
from heidgaf_core.log_config import setup_logging

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
]  # TODO: Maybe change to enum

valid_record_types = [
    "AAAA",
    "A",
]  # TODO: Maybe change to enum


class LogCollector:
    def __init__(self):
        self.log_server = {}
        self.logline = None
        self.log_data = {}

        with open(CONFIG_FILEPATH, "r") as file:
            self.config = yaml.safe_load(file)

        self.log_server["host"] = utils.validate_host(
            self.config["heidgaf"]["lc"]["logserver"]["hostname"]
        )
        self.log_server["port"] = utils.validate_port(
            self.config["heidgaf"]["lc"]["logserver"]["portout"]
        )

        self.batch_handler = KafkaBatchSender(
            topic="Prefilter", transactional_id="collector"
        )

    def fetch_logline(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.client_socket:
            self.client_socket.connect(
                (str(self.log_server.get("host")), self.log_server.get("port"))
            )
            while True:
                data = self.client_socket.recv(1024)
                if not data:
                    break
                self.logline = data.decode("utf-8")
                logger.info(f"Received logline: {self.logline}")

    def validate_and_extract_logline(self):
        if not self.logline:
            raise ValueError("Failed to extract logline: No logline.")

        parts = self.logline.split()

        try:
            if not self._check_length(parts):
                raise ValueError(
                    f"Logline does not contain exactly 8 values, but {len(parts)} values were found."
                )
            if not self._check_timestamp(parts[0]):
                raise ValueError(f"Invalid timestamp")
            if not self._check_status(parts[1]):
                raise ValueError(f"Invalid status")
            if not self._check_domain_name(parts[4]):
                raise ValueError(f"Invalid domain name")
            if not self._check_record_type(parts[5]):
                raise ValueError(f"Invalid record type")
            if not self._check_size(parts[7]):
                raise ValueError(f"Invalid size value")
        except ValueError as e:
            raise ValueError(f"Incorrect logline: {e}")

        try:
            self.log_data["client_ip"] = validate_host(parts[2])
            self.log_data["dns_ip"] = validate_host(parts[3])
            self.log_data["response_ip"] = validate_host(parts[6])
        except ValueError as e:
            self.log_data["client_ip"] = None
            self.log_data["dns_ip"] = None
            self.log_data["response_ip"] = None
            raise ValueError(f"Incorrect logline: {e}")

        self.log_data["timestamp"] = parts[0]
        self.log_data["status"] = parts[1]
        self.log_data["host_domain_name"] = parts[4]
        self.log_data["record_type"] = parts[5]
        self.log_data["size"] = parts[7]

    def add_logline_to_batch(self):
        if not self.logline or self.log_data == {}:
            raise ValueError(
                "Failed to add logline to batch: No logline or extracted data."
            )

        log_entry = self.log_data.copy()
        log_entry["client_ip"] = str(self.log_data["client_ip"])
        log_entry["dns_ip"] = str(self.log_data["dns_ip"])
        log_entry["response_ip"] = str(self.log_data["response_ip"])

        self.batch_handler.add_message(json.dumps(log_entry))

    def clear_logline(self):
        self.logline = None
        self.log_data = {}

    @staticmethod
    def _check_length(parts: list[str]) -> bool:
        return len(parts) == 8

    @staticmethod
    def _check_timestamp(timestamp: str) -> bool:
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
        if re.match(pattern, timestamp):
            return True
        return False

    @staticmethod
    def _check_status(status: str):
        return status in valid_statuses

    @staticmethod
    def _check_domain_name(domain_name: str):
        pattern = r"^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$"
        if re.match(pattern, domain_name):
            return True
        return False

    @staticmethod
    def _check_record_type(record_type: str):
        return record_type in valid_record_types

    @staticmethod
    def _check_size(size: str):
        pattern = r"^\d+b$"

        if re.match(pattern, size):
            return True
        return False


# TODO: Test
def main():
    collector = LogCollector()

    while True:
        try:
            collector.fetch_logline()
            collector.validate_and_extract_logline()
            collector.add_logline_to_batch()
        except ValueError as e:
            logger.debug(e)
        except KeyboardInterrupt:
            logger.info("Closing down LogCollector.")
            break
        finally:
            collector.clear_logline()


if __name__ == "__main__":
    main()
