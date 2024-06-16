import logging

import yaml
from confluent_kafka import KafkaError, Producer, Consumer

from heidgaf_core.config import *
from heidgaf_core.logging import setup_logging
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
class KafkaProduceHandler(KafkaHandler):
    def __init__(self):
        super().__init__()

        # conf = {
        #     'bootstrap.servers': 'localhost:9092', # TODO: Change to self.brokers
        #     'transactional.id': self.config['kafka']['producer']['transactional_id'],
        #     'acks': self.config['kafka']['producer']['acks'],
        #     'enable.idempotence': self.config['kafka']['producer']['enable_idempotence']
        # }

        conf = {'bootstrap.servers': self.brokers}

        try:
            self.producer = Producer(conf)
        except KafkaError as e:
            logger.error(f"Producer initialization failed: {e}")
            raise

    def send(self, topic: str, data: str):
        self.producer.produce(
            topic=topic,
            key=None,  # could maybe add a key here
            value=data.encode('utf-8'),
            callback=kafka_delivery_report,
        )

        self.producer.flush()


# TODO: Test
class KafkaConsumeHandler(KafkaHandler):
    def __init__(self, topics):
        super().__init__()

        # conf = {
        #     'bootstrap.servers': self.brokers,
        #     'group.id': self.config['kafka']['consumer']['group_id'],
        #     'enable.auto.commit': self.config['kafka']['consumer']['enable_auto_commit'],
        #     'auto.offset.reset': 'earliest',
        #     'enable.partition.eof': True,
        # }

        conf = {
            'bootstrap.servers': self.brokers,
            'group.id': "my_group",  # TODO: Do something with this
            'auto.offset.reset': 'earliest'
        }

        try:
            self.consumer = Consumer(conf)
            self.consumer.subscribe(topics)
        except KafkaError as e:
            logger.error(f"Consumer initialization failed: {e}")
            raise

    def __del__(self):
        if self.consumer:
            self.consumer.close()

    def receive(self) -> str:
        message = self.consumer.poll(timeout=1.0)

        if not message:
            raise KafkaMessageFetchException("No message fetched from Kafka Broker.")

        if message.error():
            raise IOError(message.error())

        self.consumer.commit(message)
        return message.value().decode('utf-8')


if __name__ == '__main__':
    handler = KafkaProduceHandler()
    handler.send("test", "TestDaten")
