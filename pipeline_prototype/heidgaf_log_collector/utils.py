import ipaddress

from confluent_kafka import KafkaError, Message


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
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
