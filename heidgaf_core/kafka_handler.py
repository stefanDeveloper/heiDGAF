# Inspired by https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/eos-transactions.py

import logging
import os  # needed for Terminal execution
import sys  # needed for Terminal execution

import yaml
from confluent_kafka import KafkaError, Producer, Consumer, TopicPartition

sys.path.append(os.getcwd())  # needed for Terminal execution
from heidgaf_core.log_config import setup_logging
from heidgaf_core.config import *
from heidgaf_core.utils import kafka_delivery_report

setup_logging()
logger = logging.getLogger(__name__)


class TooManyFailedAttemptsError(Exception):
    pass


class KafkaMessageFetchException(Exception):
    pass


class KafkaHandler:
    def __init__(self):
        self.consumer = None

        with open(CONFIG_FILEPATH, 'r') as file:
            self.config = yaml.safe_load(file)

        self.brokers = ",".join(
            [f"{broker['hostname']}:{broker['port']}" for broker in self.config['kafka']['brokers']]
        )


# TODO: Test
class KafkaProducerWrapper(KafkaHandler):
    def __init__(self):
        super().__init__()

        conf = {
            'bootstrap.servers': self.brokers,
            'transactional.id': self.config['kafka']['producer']['transactional_id'],
        }

        try:
            self.producer = Producer(conf)
            self.producer.init_transactions()
        except KafkaError as e:
            logger.error(f"Producer initialization failed: {e}")
            raise

    def send(self, topic: str, data: str):
        self.producer.begin_transaction()
        try:
            self.producer.produce(
                topic=topic,
                key=None,  # could maybe add a key here
                value=data.encode('utf-8'),
                callback=kafka_delivery_report,
            )
            self.producer.commit_transaction()
            logger.debug(f"Transaction for {data=} committed.")
        except Exception as e:
            logger.warning(f"Transaction for {data=} failed: {e}")
            self.producer.abort_transaction()

    def close(self):
        self.producer.flush()


# TODO: Test
class KafkaConsumeHandler(KafkaHandler):
    def __init__(self, topic):
        super().__init__()

        conf = {
            'bootstrap.servers': self.brokers,
            'group.id': self.config['kafka']['consumer']['group_id'],
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',
            'enable.partition.eof': True,
        }

        try:
            self.consumer = Consumer(conf)
            self.consumer.assign([TopicPartition(topic, 0)])
        except KafkaError as e:
            logger.error(f"Consumer initialization failed: {e}")
            raise

    def __del__(self):
        if self.consumer:
            self.consumer.close()

    def consume(self) -> tuple[str | None, str | None]:
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break
                key = msg.key().decode('utf-8') if msg.key() else None
                value = msg.value().decode('utf-8') if msg.value() else None
                logger.info(f"Received message: Key={key}, Value={value}")
                self.consumer.commit(msg)
                return key, value
        except KeyboardInterrupt:
            logger.info("Shutting down Kafka Consume Handler...")
            raise KeyboardInterrupt
        except Exception as e:
            logger.error(f"Error in Kafka Consume Handler: {e}")


if __name__ == '__main__':
    producer_handler = KafkaProducerWrapper()
    producer_handler.send("test", "Test")

    consumer_handler = KafkaConsumeHandler("test")
    consumer_handler.consume()
