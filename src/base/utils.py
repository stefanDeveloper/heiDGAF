import ipaddress
import logging
import os
import sys

import yaml
from confluent_kafka import KafkaError, Message

sys.path.append(os.getcwd())
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
    host: int | str | bytes | ipaddress.IPv4Address | ipaddress.IPv6Address,
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
    except Exception as err:
        raise ValueError(f"Invalid host: {host}, {err=}")

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
        TypeError: Invalid type for port number, must be int
    """
    logger.debug(f"Validating port {port}...")
    if not isinstance(port, int):
        raise TypeError

    if not (1 <= port <= 65535):
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
        logger.debug(
            "Message delivered to topic={} [partition={}]".format(
                msg.topic(), msg.partition()
            )
        )


def get_first_part_of_ipv4_address(
    address: ipaddress.IPv4Address, length: int
) -> ipaddress.IPv4Address:
    """
    Returns the first part of an IPv4 address, the rest is filled with 0. For example:
    >>> get_first_part_of_ipv4_address(ipaddress.IPv4Address("255.255.255.255"), 23)
    IPv4Address('255.255.254.0')
    >>> get_first_part_of_ipv4_address(ipaddress.IPv4Address("172.126.15.3"), 8)
    IPv4Address('172.0.0.0')

    Args:
        address (ipaddress.IPv4Address): The IPv4 Address to get the first part of
        length (int): Length of the first part, the other ``32 - length`` bits are set to 0

    Returns:
        IPv4Address with first ``length`` bits kept, others set to 0
    """
    if length < 0 or length > 32:
        raise ValueError("Invalid length. Must be between 0 and 32.")

    if isinstance(address, ipaddress.IPv4Address):
        binary_string = "".join(format(byte, "08b") for byte in address.packed)
        first_part_binary = binary_string[:length]
        first_part_binary_padded = first_part_binary.ljust(32, "0")
        first_part_address = ipaddress.IPv4Address(int(first_part_binary_padded, 2))
    else:
        raise ValueError("Invalid IP address format")

    return first_part_address
