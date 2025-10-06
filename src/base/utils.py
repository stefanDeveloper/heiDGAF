import ipaddress
import os
import sys
from typing import Optional

import yaml
from confluent_kafka import KafkaError, Message

sys.path.append(os.getcwd())
from src.base.log_config import get_logger

logger = get_logger()

CONFIG_FILEPATH = os.path.join(os.path.dirname(__file__), "../../config.yaml")


def setup_config():
    """Load and return the application configuration from the YAML configuration file.

    Reads the configuration file from the predefined CONFIG_FILEPATH and parses
    it as a YAML document. This function provides centralized configuration
    loading for the entire application.

    Returns:
        dict: Configuration data as a Python dictionary containing all
              application settings and parameters.

    Raises:
        FileNotFoundError: If the configuration file does not exist at the
                           expected path.
        yaml.YAMLError: If the configuration file contains invalid YAML syntax.
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
    """Validate and normalize a host IP address.

    Accepts various input formats for IP addresses and validates them using
    the ipaddress module. Returns a properly typed IP address object for
    further processing.

    Args:
        host (int | str | bytes | IPv4Address | IPv6Address): Host IP address
            in any supported format (string, integer, bytes, or existing
            IP address object).

    Returns:
        ipaddress.IPv4Address | ipaddress.IPv6Address: Validated IP address
            object with the appropriate type.

    Raises:
        ValueError: If the provided host is not a valid IP address format.
    """
    logger.debug(f"Validating host IP address {host}...")
    try:
        host = ipaddress.ip_address(host)
    except Exception as err:
        raise ValueError(f"Invalid host: {host}, {err=}")

    logger.debug(f"Host {host} is valid.")
    return host


def validate_port(port: int) -> int:
    """Validate that a port number is within the valid range.

    Checks if the provided port number is an integer and falls within
    the valid TCP/UDP port range (1-65535). Returns the validated port
    number if valid.

    Args:
        port (int): Port number to validate.

    Returns:
        int: Validated port number.

    Raises:
        TypeError: If port is not an integer.
        ValueError: If port number is not in the valid range (1-65535).
    """
    logger.debug(f"Validating port {port}...")
    if not isinstance(port, int):
        raise TypeError

    if not (1 <= port <= 65535):
        raise ValueError(f"Invalid port: {port}")

    logger.debug(f"Port {port} is valid.")
    return port


def kafka_delivery_report(err: Optional[KafkaError], msg: Optional[Message]):
    """Callback function for Kafka message delivery reports

    Used by Kafka Producers to handle delivery confirmations and errors.
    Logs successful deliveries with topic and partition information, and
    warns about delivery failures.

    Args:
        err (Optional[KafkaError]): Error object if delivery failed, None if successful.
        msg (Optional[Message]): Message object containing delivery details, None if error.
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
    """Extract the network portion of an IPv4 address using the specified prefix length.

    Creates a subnet identifier by zeroing out the host portion of the IP address
    based on the provided prefix length. This is useful for network analysis
    and grouping IP addresses by subnet.

    Args:
        address (ipaddress.IPv4Address): The IPv4 address to normalize.
        prefix_length (int): CIDR prefix length (0-32) defining the network portion.

    Returns:
        tuple[ipaddress.IPv4Address, int]: A tuple containing:
            - Network address with host bits set to zero.
            - The prefix length used for normalization.

    Raises:
        ValueError: If prefix_length is not in the valid range (0-32).
    """
    if not (0 <= prefix_length <= 32):
        raise ValueError("Invalid prefix length for IPv4. Must be between 0 and 32.")

    net = ipaddress.IPv4Network((address, prefix_length), strict=False)
    return net.network_address, prefix_length


def normalize_ipv6_address(
    address: ipaddress.IPv6Address, prefix_length: int
) -> tuple[ipaddress.IPv6Address, int]:
    """Extract the network portion of an IPv6 address using the specified prefix length.

    Creates a subnet identifier by zeroing out the host portion of the IP address
    based on the provided prefix length. This is useful for network analysis
    and grouping IPv6 addresses by subnet.

    Args:
        address (ipaddress.IPv6Address): The IPv6 address to normalize.
        prefix_length (int): CIDR prefix length (0-128) defining the network portion.

    Returns:
        tuple[ipaddress.IPv6Address, int]: A tuple containing:
            - Network address with host bits set to zero.
            - The prefix length used for normalization.

    Raises:
        ValueError: If prefix_length is not in the valid range (0-128).
    """
    if not (0 <= prefix_length <= 128):
        raise ValueError("Invalid prefix length for IPv6. Must be between 0 and 128.")

    net = ipaddress.IPv6Network((address, prefix_length), strict=False)
    return net.network_address, prefix_length
