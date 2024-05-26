import ipaddress
import logging

from confluent_kafka import KafkaError, Message

from pipeline_prototype.logging_config import setup_logging

# from confluent_kafka.admin import AdminClient, import NewTopic

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


def get_first_part_of_ipv4_address(address: ipaddress.IPv4Address, length: int) -> ipaddress.IPv4Address:
    if length < 0 or length > 32:
        raise ValueError("Invalid length. Must be between 0 and 32.")

    if isinstance(address, ipaddress.IPv4Address):
        binary_string = ''.join(format(byte, '08b') for byte in address.packed)
        first_part_binary = binary_string[:length]
        first_part_binary_padded = first_part_binary.ljust(32, '0')
        first_part_address = ipaddress.IPv4Address(int(first_part_binary_padded, 2))
        return first_part_address
    else:
        raise ValueError("Invalid IP address format")

# UNTESTED:
# def create_kafka_topic(broker_host, broker_port, topic_name, num_partitions=1, replication_factor=1):
#     admin_client = AdminClient({'bootstrap.servers': f"{broker_host}:{broker_port}"})
#
#     topic_list = [NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
#
#     try:
#         fs = admin_client.create_topics(topic_list)
#         for topic, f in fs.items():
#             try:
#                 f.result()
#                 print(f"Topic {topic} created successfully")
#             except KafkaException as e:
#                 print(f"Failed to create topic {topic}: {e}")
#     except Exception as e:
#         print(f"Exception while creating topic: {e}")
#
#
# create_kafka_topic("localhost", "9092", "192.168.0.x")
