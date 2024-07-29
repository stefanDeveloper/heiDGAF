import ipaddress
import logging
import os
import sys
from datetime import datetime

import yaml
from confluent_kafka import KafkaError, Message

sys.path.append(os.path.abspath('../..'))  # needed for Terminal execution
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

CONFIG_FILEPATH = os.path.join(os.path.dirname(__file__), "../../config.yaml")


def setup_config():
    """
    Loads the configuration data from the configuration file and returns it as the corresponding Python object.

    Returns:
         Configuration data as corresponding Python object

    Raises:
        FileNotFoundError: Configuration file could not be opened
    """
    try:
        logger.debug(f"Opening configuration file at {CONFIG_FILEPATH}...")
        with open(CONFIG_FILEPATH, "r") as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logger.critical(f"File {CONFIG_FILEPATH} does not exist. Aborting...")
        raise

    logger.debug("Configuration file successfully opened and information returned.")
    return config


def validate_host(
        host: int | str | bytes | ipaddress.IPv4Address | ipaddress.IPv6Address
) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    """
    Checks if the given host is a valid IP address. If it is, the IP address is returned with IP address type.

    Args:
        host (int | str | bytes | IPv4Address | IPv6Address): Host IP address to be checked

    Returns:
        Correct IP address as ipaddress.IPv4Address or ipaddress.IPv6Address type.

    Raises:
        ValueError: Invalid host IP address format
    """
    logger.debug(f"Validating host IP address {host}...")
    try:
        host = ipaddress.ip_address(host)
    except ValueError:
        logger.error(f"Host {host} is invalid.")
        raise ValueError(f"Invalid host: {host}")

    logger.debug(f"Host {host} is valid.")
    return host


def validate_port(port: int) -> int:
    """
    Checks if the given port number is in the valid port number range. If it is, the port is returned.

    Args:
        port (int): Port number to be checked

    Returns:
        Validated port number as integer

    Raises:
        ValueError: Port number not in valid port number range
    """
    logger.debug(f"Validating port {port}...")
    if not isinstance(port, int):
        raise TypeError

    if not (1 <= port <= 65535):
        logger.error(f"Port {port} is invalid.")
        raise ValueError(f"Invalid port: {port}")

    logger.debug(f"Port {port} is valid.")
    return port


def kafka_delivery_report(err: None | KafkaError, msg: None | Message):
    """
    Delivery report used by Kafka Producers. Specifies the format of the returned messages during producing.
    """
    if err:
        logger.warning("Message delivery failed: {}".format(err))
    else:
        logger.info(
            "Message delivered to topic={} [partition={}]".format(
                msg.topic(), msg.partition()
            )
        )


def current_time() -> str:
    """
    Returns the current time.

    Returns:
        Returns the timestamp of now correctly formatted as string.
    """
    logger.debug("Returning current timestamp...")
    # TODO: Replace deprecated method
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
