import json
import logging
import os  # needed for Terminal execution
import re
import socket
import sys

import yaml

sys.path.append(os.getcwd())  # needed for Terminal execution
from src.base import utils
from src.base.batch_handler import KafkaBatchSender
from src.base.config import CONFIG_FILEPATH  # needed for Terminal execution
from src.base.log_config import setup_logging
from src.base.utils import validate_host

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
    def __init__(self):
        logger.debug("Initializing LogCollector...")
        self.log_server = {}
        self.logline = None
        self.log_data = {}

        try:
            logger.debug(f"Opening configuration file at {CONFIG_FILEPATH}...")
            with open(CONFIG_FILEPATH, "r") as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            logger.critical(f"File {CONFIG_FILEPATH} not does not exist. Aborting...")
            raise
        logger.debug("Configuration file successfully opened and information stored.")

        self.log_server["host"] = utils.validate_host(
            self.config["heidgaf"]["lc"]["logserver"]["hostname"]
        )
        logger.debug(f"LogServer host was set to {self.log_server['host']}.")

        self.log_server["port"] = utils.validate_port(
            self.config["heidgaf"]["lc"]["logserver"]["portout"]
        )
        logger.debug(f"LogServer outgoing port was set to {self.log_server['port']}.")

        logger.debug(
            f"Calling KafkaBatchSender(topic=Prefilter, transactional_id=collector)..."
        )
        self.batch_handler = KafkaBatchSender(
            topic="Prefilter", transactional_id="collector"
        )
        logger.debug("Initialized LogCollector.")

    def fetch_logline(self):
        logger.debug("Fetching new logline from LogServer...")
        try:
            with socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM
            ) as self.client_socket:
                logger.debug(
                    f"Trying to connect to LogServer ({self.log_server['host']}:{self.log_server['port']})..."
                )
                self.client_socket.connect(
                    (str(self.log_server.get("host")), self.log_server.get("port"))
                )
                logger.debug("Connected to LogServer. Retrieving data...")

                data = self.client_socket.recv(1024)  # log lines are at most ~150 bytes long

                if not data:
                    logger.debug("No data available on LogServer.")
                    return

                self.logline = data.decode("utf-8")
                logger.info(f"Received logline.")
                logger.debug(f"{self.logline=}")
        except ConnectionError:
            logger.error(
                f"Could not connect to LogServer ({self.log_server['host']}:{self.log_server['port']})."
            )
            raise

    def validate_and_extract_logline(self):
        logger.debug("Validating and extracting current logline...")
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
        logger.debug(f"Timestamp set to {self.log_data['timestamp']}")
        self.log_data["status"] = parts[1]
        logger.debug(f"Status set to {self.log_data['status']}")
        self.log_data["host_domain_name"] = parts[4]
        logger.debug(f"Host domain name set to {self.log_data['host_domain_name']}")
        self.log_data["record_type"] = parts[5]
        logger.debug(f"Record type set to {self.log_data['record_type']}")
        self.log_data["size"] = parts[7]
        logger.debug(f"Size set to {self.log_data['size']}")
        logger.debug("All data extracted from logline.")

    def add_logline_to_batch(self):
        logger.debug("Adding logline to batch...")
        if not self.logline or self.log_data == {}:
            raise ValueError(
                "Failed to add logline to batch: No logline or extracted data."
            )

        log_entry = self.log_data.copy()
        log_entry["client_ip"] = str(self.log_data["client_ip"])
        log_entry["dns_ip"] = str(self.log_data["dns_ip"])
        log_entry["response_ip"] = str(self.log_data["response_ip"])

        logger.debug("Calling KafkaBatchSender to add message...")
        self.batch_handler.add_message(json.dumps(log_entry))
        logger.debug("Added message to batch.")

    def clear_logline(self):
        logger.debug("Clearing current logline...")
        self.logline = None
        self.log_data = {}
        logger.debug("Cleared logline.")

    @staticmethod
    def _check_length(parts: list[str]) -> bool:
        logger.debug(f"Checking the size of the given list {parts}...")
        size = len(parts)
        allowed_size = 8
        return_value = size == allowed_size

        if return_value:
            logger.debug("Size of given list is valid.")
        else:
            logger.warning(
                f"Size of given list is invalid. Is: {size}, Should be: {allowed_size}."
            )

        return return_value

    @staticmethod
    def _check_timestamp(timestamp: str) -> bool:
        logger.debug(f"Checking timestamp format of {timestamp}...")
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"

        if re.match(pattern, timestamp):
            logger.debug(f"Timestamp {timestamp} is correctly formatted.")
            return True

        logger.warning(f"Timestamp {timestamp} is incorrectly formatted.")
        return False

    @staticmethod
    def _check_status(status: str):
        logger.debug(f"Checking status code {status}...")
        return_value = status in valid_statuses

        if return_value:
            logger.debug(f"Status code {status} is valid.")
        else:
            logger.warning(
                f"Status code {status} is invalid: Allowed status codes: {valid_statuses}."
            )

        return return_value

    @staticmethod
    def _check_domain_name(domain_name: str):
        logger.debug(f"Checking domain name {domain_name}...")
        pattern = r"^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$"

        if re.match(pattern, domain_name):
            logger.debug(f"Domain name {domain_name} is correct.")
            return True

        logger.warning(f"Domain name {domain_name} is incorrect.")
        return False

    @staticmethod
    def _check_record_type(record_type: str) -> bool:
        logger.debug(f"Checking record type {record_type}...")
        return_value = record_type in valid_record_types

        if return_value:
            logger.debug(f"Record type {record_type} is valid.")
        else:
            logger.warning(
                f"Record type {record_type} is invalid: Allowed record types: {valid_record_types}."
            )

        return return_value

    @staticmethod
    def _check_size(size: str):
        logger.debug(f"Checking size value {size}...")
        pattern = r"^\d+b$"

        if re.match(pattern, size):
            logger.debug(f"Size value {size} is valid.")
            return True

        logger.debug(f"Size value {size} is invalid.")
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
