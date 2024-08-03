import json
import logging
import os  # needed for Terminal execution
import re
import socket
import sys

sys.path.append(os.getcwd())  # needed for Terminal execution
from src.base import utils
from src.base.batch_handler import CollectorKafkaBatchSender
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

config = utils.setup_config()
VALID_STATUS_CODES = config["heidgaf"]["collector"]["valid_status_codes"]
VALID_RECORD_TYPES = config["heidgaf"]["collector"]["valid_record_types"]
LOGSERVER_HOSTNAME = config["heidgaf"]["logserver"]["hostname"]
LOGSERVER_SENDING_PORT = config["heidgaf"]["logserver"]["port_out"]
SUBNET_BITS = config["heidgaf"]["subnet"]["subnet_bits"]


class LogCollector:
    """
    Connects to the :class:`LogServer`'s outgoing port to receive a log line. Validates all data fields by type and
    value, invalid log lines are discarded. All valid log lines are sent to the :class:`CollectorKafkaBatchSender`.
    """

    def __init__(self):
        logger.debug("Initializing LogCollector...")
        self.log_server = {}
        self.logline = None
        self.log_data = {}

        self.log_server["host"] = utils.validate_host(LOGSERVER_HOSTNAME)
        logger.debug(f"LogServer host was set to {self.log_server['host']}.")

        self.log_server["port"] = utils.validate_port(LOGSERVER_SENDING_PORT)
        logger.debug(f"LogServer outgoing port was set to {self.log_server['port']}.")

        logger.debug(
            f"Calling CollectorKafkaBatchSender(transactional_id=collector)..."
        )
        self.batch_handler = CollectorKafkaBatchSender()
        logger.debug("Initialized LogCollector.")

    def fetch_logline(self) -> None:
        """
        Connects to the :class:`LogServer` and fetches a log line. If log line is available, it is decoded and stored.

        Raises:
            ConnectionError: Connection to :class:`LogServer` cannot be established.
        """
        logger.debug("Fetching new log line from LogServer...")
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
                logger.info(f"Received log line.")
                logger.debug(f"{self.logline=}")
        except ConnectionError:
            logger.error(
                f"Could not connect to LogServer ({self.log_server['host']}:{self.log_server['port']})."
            )
            raise

    def validate_and_extract_logline(self) -> None:
        """
        Splits the log line into its respective fields and validates all fields. Validation of a field is specific
        to the type of field (e.g. IP Address is checked for correct format). Stores the validated fields internally.

        Raises:
            ValueError: Log line has one or multiple invalid fields, or a wrong number of fields
        """
        logger.debug("Validating and extracting current log line...")
        if not self.logline:
            raise ValueError("Failed to extract logline: No log line.")

        parts = self.logline.split()

        try:
            if not self._check_length(parts):
                raise ValueError(
                    f"Log line does not contain exactly 8 values, but {len(parts)} values were found."
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
            raise ValueError(f"Incorrect log line: {e}")

        try:
            self.log_data["client_ip"] = utils.validate_host(parts[2])
            self.log_data["dns_ip"] = utils.validate_host(parts[3])
            self.log_data["response_ip"] = utils.validate_host(parts[6])
        except ValueError as e:
            self.log_data["client_ip"] = None
            self.log_data["dns_ip"] = None
            self.log_data["response_ip"] = None
            raise ValueError(f"Incorrect log line: {e}")

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
        logger.info("Log line is correct. Extracted all data.")

    def add_logline_to_batch(self) -> None:
        """
        Sends the validated log line in JSON format to :class:`CollectorKafkaBatchSender`, where it is stored in
        a temporary batch before being sent to topic ``Prefilter``.
        """
        logger.debug("Adding log line to batch...")
        if not self.logline or self.log_data == {}:
            raise ValueError(
                "Failed to add log line to batch: No logline or extracted data."
            )

        log_entry = self.log_data.copy()
        log_entry["client_ip"] = str(self.log_data["client_ip"])
        log_entry["dns_ip"] = str(self.log_data["dns_ip"])
        log_entry["response_ip"] = str(self.log_data["response_ip"])

        logger.debug("Calling KafkaBatchSender to add message...")
        subnet_id = f"{utils.get_first_part_of_ipv4_address(self.log_data['client_ip'], SUBNET_BITS)}_{SUBNET_BITS}"

        self.batch_handler.add_message(subnet_id, json.dumps(log_entry))
        logger.info("Added message to batch. Data will be sent when the batch is full.")
        logger.debug(f"{log_entry=}")
        logger.debug(f"{json.dumps(log_entry)=}")

    def clear_logline(self) -> None:
        """
        Clears all information regarding the stored log line. Afterward, instance can load the next log line.
        """
        logger.debug("Clearing current log line...")
        self.logline = None
        self.log_data = {}
        logger.debug("Cleared log line.")

    @staticmethod
    def _check_length(parts: list[str]) -> bool:
        """
        Validates the number of fields of the log line.

        Args:
            parts (list[str]): Fields of the log line, already split up.

        Returns:
            ``True`` if number of fields is valid, ``False`` otherwise
        """
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
        """
        Validates the timestamp format. Correct format described in
        https://heidgaf.readthedocs.io/en/latest/pipeline.html#logcollector.

        Args:
            timestamp (str): Timestamp to be checked.

        Returns:
            ``True`` if timestamp (format) is correct, ``False`` otherwise
        """
        logger.debug(f"Checking timestamp format of {timestamp}...")
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"

        if re.match(pattern, timestamp):
            logger.debug(f"Timestamp {timestamp} is correctly formatted.")
            return True

        logger.warning(f"Timestamp {timestamp} is incorrectly formatted.")
        return False

    @staticmethod
    def _check_status(status: str) -> bool:
        """
        Validates the status type of the log line.

        Args:
            status (str): Status type to be checked.

        Returns:
            ``True`` if the given status type is in ``VALID_STATUS_CODES``, ``False`` otherwise
        """
        logger.debug(f"Checking status code {status}...")
        return_value = status in VALID_STATUS_CODES

        if return_value:
            logger.debug(f"Status code {status} is valid.")
        else:
            logger.warning(
                f"Status code {status} is invalid: Allowed status codes: {VALID_STATUS_CODES}."
            )

        return return_value

    @staticmethod
    def _check_domain_name(domain_name: str) -> bool:
        """
        Validates the format of the given domain name.

        Args:
            domain_name (str): Domain name to be checked.

        Returns:
            ``True`` if the given domain name's format is valid, ``False`` otherwise
        """
        logger.debug(f"Checking domain name {domain_name}...")
        pattern = r"^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$"

        if re.match(pattern, domain_name):
            logger.debug(f"Domain name {domain_name} is correct.")
            return True

        logger.warning(f"Domain name {domain_name} is incorrect.")
        return False

    @staticmethod
    def _check_record_type(record_type: str) -> bool:
        """
        Validates the record type.

        Args:
            record_type (str): Record type to be checked.

        Returns:
            ``True`` if given string is a valid record type, ``False`` otherwise
        """
        logger.debug(f"Checking record type {record_type}...")
        return_value = record_type in VALID_RECORD_TYPES

        if return_value:
            logger.debug(f"Record type {record_type} is valid.")
        else:
            logger.warning(
                f"Record type {record_type} is invalid: Allowed record types: {VALID_RECORD_TYPES}."
            )

        return return_value

    @staticmethod
    def _check_size(size: str) -> bool:
        """
        Validates the size value. Size value is given is bytes, with an additional 'b' after the number.

        Args:
            size (str): Size value to be checked.

        Returns:
            ``True`` if the size value has the correct format, ``False`` otherwise
        """
        logger.debug(f"Checking size value {size}...")
        pattern = r"^\d+b$"

        if re.match(pattern, size):
            logger.debug(f"Size value {size} is valid.")
            return True

        logger.debug(f"Size value {size} is invalid.")
        return False


# TODO: Test
def main() -> None:
    """
    Creates the :class:`LogCollector` instance. Starts a loop that continuously fetches a log line, validates and
    extracts its information and adds it to the batch if valid.

    Raises:
        KeyboardInterrupt: Execution interrupted by user. Closes down the :class:`LogCollector` instance.
    """
    logger.info("Starting LogCollector...")
    collector = LogCollector()
    logger.info("LogCollector started. Fetching log lines from LogServer...")

    while True:
        try:
            logger.debug("Before fetching log line")
            collector.fetch_logline()
            logger.debug("After fetching log line")

            logger.debug("Before validating and extracting log line")
            collector.validate_and_extract_logline()
            logger.debug("After validating and extracting log line")

            logger.debug("Before adding log line to batch")
            collector.add_logline_to_batch()
            logger.debug("After adding log line to batch")
        except ValueError as e:
            logger.debug("Incorrect log line: Waiting for next log line...")
            logger.debug(e)
        except KeyboardInterrupt:
            logger.info("Closing down LogCollector...")
            collector.clear_logline()
            logger.info("LogCollector closed down.")
            break
        finally:
            logger.debug("Closing down LogCollector...")
            collector.clear_logline()


if __name__ == "__main__":
    main()
