import ipaddress
import logging
import os
import sys
from datetime import datetime

import yaml
from confluent_kafka import KafkaError, Message

sys.path.append(os.getcwd())
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# TODO: Update
CONFIG_FILEPATH = os.path.join(os.path.dirname(__file__), "../../config.yaml")


def setup_config():
    try:
        logger.debug(f"Opening configuration file at {CONFIG_FILEPATH}...")
        with open(CONFIG_FILEPATH, "r") as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logger.critical(f"File {CONFIG_FILEPATH} does not exist. Aborting...")
        raise
    logger.debug("Configuration file successfully opened and information returned.")
    return config


def validate_host(host) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    logger.debug(f"Validating host IP address {host}...")
    try:
        host = ipaddress.ip_address(host)
    except ValueError:
        logger.error(f"Host {host} is invalid.")
        raise ValueError(f"Invalid host: {host}")

    logger.debug(f"Host {host} is valid.")
    return host


def validate_port(port: int) -> int:
    logger.debug(f"Validating port {port}...")
    if not isinstance(port, int):
        raise TypeError

    if not (1 <= port <= 65535):
        logger.error(f"Port {port} is invalid.")
        raise ValueError(f"Invalid port: {port}")

    logger.debug(f"Port {port} is valid.")
    return port


def kafka_delivery_report(err: None | KafkaError, msg: None | Message):
    if err:
        logger.warning("Message delivery failed: {}".format(err))
    else:
        logger.info(
            "Message delivered to topic={} [partition={}]".format(
                msg.topic(), msg.partition()
            )
        )


def current_time():
    logger.debug("Returning current timestamp...")
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
