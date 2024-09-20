import json
import logging
import os
import socket
import sys

from base.logline_handler import LoglineHandler

sys.path.append(os.getcwd())
from src.base import utils
from src.logcollector.batch_handler import CollectorKafkaBatchSender
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

config = utils.setup_config()
VALID_STATUS_CODES = config["heidgaf"]["collector"]["valid_status_codes"]
VALID_RECORD_TYPES = config["heidgaf"]["collector"]["valid_record_types"]
LOGSERVER_HOSTNAME = config["heidgaf"]["logserver"]["hostname"]
LOGSERVER_SENDING_PORT = config["heidgaf"]["logserver"]["port_out"]
SUBNET_BITS = config["heidgaf"]["subnet"]["subnet_bits"]
BATCH_SIZE = config["kafka"]["batch_sender"]["batch_size"]


class LogCollector:
    """
    Connects to the :class:`LogServer`'s outgoing port to receive a logline. Validates all data fields by type and
    value, invalid loglines are discarded. All valid loglines are sent to the :class:`CollectorKafkaBatchSender`.
    """

    def __init__(self):
        logger.debug("Initializing LogCollector...")
        self.log_server = {}
        self.logline = None

        self.log_server["host"] = utils.validate_host(LOGSERVER_HOSTNAME)
        logger.debug(f"LogServer host was set to {self.log_server['host']}.")

        self.log_server["port"] = utils.validate_port(LOGSERVER_SENDING_PORT)
        logger.debug(f"LogServer outgoing port was set to {self.log_server['port']}.")

        logger.debug(
            f"Calling CollectorKafkaBatchSender(transactional_id='collector')..."
        )
        self.batch_handler = CollectorKafkaBatchSender()
        logger.debug(
            f"Calling LoglineHandler()..."
        )
        self.logline_handler = LoglineHandler()
        logger.debug("Initialized LogCollector.")

    def fetch_logline(self) -> None:
        """
        Connects to the :class:`LogServer` and fetches a logline. If logline is available, it is decoded and stored.

        Raises:
            ConnectionError: Connection to :class:`LogServer` cannot be established.
        """
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

                data = self.client_socket.recv(1024)  # loglines are at most ~150 bytes long

                if not data:
                    logger.debug("No data available on LogServer.")
                    return

                self.logline = data.decode("utf-8")
                logger.info(f"Received message:\n    ⤷  {self.logline}")
        except ConnectionError:
            logger.error(
                f"Could not connect to LogServer ({self.log_server['host']}:{self.log_server['port']})."
            )
            raise

    def add_logline_to_batch(self) -> None:
        """
        Sends the validated logline in JSON format to :class:`CollectorKafkaBatchSender`, where it is stored in
        a temporary batch before being sent to topic ``Prefilter``. Adds a subnet_id to the message, that it retrieves
        from the client's IP address.
        """
        logger.debug("Adding logline to batch...")
        if not self.logline:
            raise ValueError(
                "Failed to add logline to batch: No logline."
            )

        log_data = self.logline_handler.validate_logline_and_get_fields_as_json(self.logline)

        logger.debug("Calling KafkaBatchSender to add message...")
        subnet_id = f"{utils.get_first_part_of_ipv4_address(log_data.get('client_ip'), SUBNET_BITS)}_{SUBNET_BITS}"

        self.batch_handler.add_message(subnet_id, json.dumps(log_data))

        logger.info(
            "Added message to the batch.\n"
            f"    ⤷  The subnet_id {subnet_id} batch currently stores "
            f"{self.batch_handler.batch.get_number_of_messages(subnet_id)} of {BATCH_SIZE} messages."
        )
        logger.debug(f"{log_data=}")
        logger.debug(f"{json.dumps(log_data)=}")

    def clear_logline(self) -> None:
        """
        Clears all information regarding the stored logline. Afterward, instance can load the next logline.
        """
        logger.debug("Clearing current logline...")
        self.logline = None
        logger.debug("Cleared logline.")


def main(one_iteration: bool = False) -> None:
    """
    Creates the :class:`LogCollector` instance. Starts a loop that continuously fetches a logline, validates and
    extracts its information and adds it to the batch if valid.

    Args:
        one_iteration (bool): For testing purposes: stops loop after one iteration

    Raises:
        KeyboardInterrupt: Execution interrupted by user. Closes down the :class:`LogCollector` instance.
    """
    logger.info("Starting LogCollector...")
    collector = LogCollector()
    logger.info("LogCollector started.\n"
                "    ⤷  Fetching loglines from LogServer...\n"
                "    ⤷  Data will be sent when the respective batch is full or the global timer runs out.")

    iterations = 0

    while True:
        if one_iteration and iterations > 0:
            break
        iterations += 1

        try:
            logger.debug("Before fetching logline")
            collector.fetch_logline()
            logger.debug("After fetching logline")

            logger.debug("Before adding logline to batch")
            collector.add_logline_to_batch()
            logger.debug("After adding logline to batch")
        except ValueError as e:
            logger.debug("Incorrect logline: Waiting for next logline...")
            logger.debug(e)
        except KeyboardInterrupt:
            logger.info("Closing down LogCollector...")
            collector.clear_logline()
            logger.info("LogCollector closed down.")
            break
        finally:
            logger.debug("Closing down LogCollector...")
            collector.clear_logline()


if __name__ == "__main__":  # pragma: no cover
    main()
