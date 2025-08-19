import ipaddress
import os
import sys
import uuid
import yaml
from confluent_kafka import KafkaError, Message

sys.path.append(os.getcwd())
from src.base.log_config import get_logger

logger = get_logger()

CONFIG_FILEPATH = os.path.join(os.path.dirname(__file__), "../../config.yaml")


def get_zeek_sensor_topic_base_names(config: dict) -> set:
    """
    Method to retrieve the protocols monitored by the zeek sensors based on the ``config.yaml``
    
    Args:
        config (dict): The configuration dictionary from config.yaml
        
    Returns:
        Set of protocol names the zeek sensors are monitoring, e.g. (dns, http, sftp, ... )
    """
    return set(
        [
            protocol
            for sensor in config["pipeline"]["zeek"]["sensors"].values()
            for protocol in sensor.get("protocols", [])
        ]
    )

# TODO: test this method!
def get_batch_configuration(collector_name: str) -> dict:
    """
    Method to combine custom batch_handler configuartions per logcollector with the default ones. 
    Yields a dict where custom configurations override default ones. If no custom value is specified,
    deafult values are returned.
    
    Args:
        collector_name (str): Name of the collector to retrieve the configuration for
    Returns:
        Dictionairy with the complete batch_handler configuration (e.g. ipv4_prefix_length, batch_size, etc. )
    """
    config = setup_config()
    default_configuration = config["pipeline"]["log_collection"][
        "default_batch_handler_config"
    ]
    collector_configs = config["pipeline"]["log_collection"]["collectors"]

    for collector in collector_configs:
        if collector["name"] == collector_name:
            override = collector.get("batch_handler_config_override")
            if override:
                # Merge override into a copy of the default configuration
                merged = {**default_configuration, **override}
                return merged

    return default_configuration


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
    Returns the first part of an IPv4 address, the rest is filled with 0.

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


def generate_collisions_resistant_uuid():
    return f"{uuid.uuid4()}-{uuid.uuid4()}"
