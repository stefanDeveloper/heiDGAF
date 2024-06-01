import ipaddress
import logging

from confluent_kafka import KafkaError, Message

from heidgaf_log_collector.logging_config import setup_logging

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
# create_kafka_topic("localhost", "9092", "Prefilter")
# create_kafka_topic("localhost", "9092", "Inspect")
