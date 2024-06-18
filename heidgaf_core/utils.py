import ipaddress
import logging
import os
import sys

from confluent_kafka import KafkaError, Message

sys.path.append(os.getcwd())
from heidgaf_core.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def validate_host(host) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    try:
        host = ipaddress.ip_address(host)
    except ValueError:
        raise ValueError(f"Invalid host: {host}")
    return host


def validate_port(port: int) -> int:
    if not isinstance(port, int):
        raise TypeError

    if not (1 <= port <= 65535):
        raise ValueError(f"Invalid port: {port}")
    return port


def kafka_delivery_report(err: None | KafkaError, msg: None | Message):
    if err:
        logger.warning('Message delivery failed: {}'.format(err))
    else:
        logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
