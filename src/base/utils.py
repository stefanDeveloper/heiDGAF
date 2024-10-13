import ipaddress
import os
import sys

import yaml
from confluent_kafka import KafkaError, Message

sys.path.append(os.getcwd())
from src.base.log_config import get_logger

logger = get_logger()

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


def normalize_ipv4_address(
    address: ipaddress.IPv4Address, prefix_length: int
) -> tuple[ipaddress.IPv4Address, int]:
    """
    Returns the first part of an IPv4 address, the rest is filled with 0. For example:
    >>> normalize_ipv4_address(ipaddress.IPv4Address("255.255.255.255"), 23)
    (IPv4Address('255.255.254.0'), 23)
    >>> normalize_ipv4_address(ipaddress.IPv4Address("172.126.15.3"), 8)
    (IPv4Address('172.0.0.0'), 8)

    Args:
        address (ipaddress.IPv4Address): The IPv4 address to get the subnet ID of
        prefix_length (int): Prefix length to be used for the subnet ID

    Returns:
        Subnet ID of the given IP address
    """
    if not (0 <= prefix_length <= 32):
        raise ValueError("Invalid prefix length for IPv4. Must be between 0 and 32.")

    net = ipaddress.IPv4Network((address, prefix_length), strict=False)
    return net.network_address, prefix_length


def normalize_ipv6_address(
    address: ipaddress.IPv6Address, prefix_length: int
) -> tuple[ipaddress.IPv6Address, int]:
    """
    Returns the first part of an IPv6 address, the rest is filled with 0.

    Args:
        address (ipaddress.IPv6Address): The IPv6 address to get the subnet ID of
        prefix_length (int): Prefix length to be used for the subnet ID

    Returns:
        Subnet ID of the given IP address
    """
    if not (0 <= prefix_length <= 128):
        raise ValueError("Invalid prefix length for IPv6. Must be between 0 and 128.")

    net = ipaddress.IPv6Network((address, prefix_length), strict=False)
    return net.network_address, prefix_length
