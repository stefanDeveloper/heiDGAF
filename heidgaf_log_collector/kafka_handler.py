import logging

import yaml
from confluent_kafka import KafkaError, Message, Producer, Consumer, KafkaException

from heidgaf_log_collector.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class KafkaMessageFetchException(Exception):
    pass


# TODO: Test
class KafkaHandler:
    def __init__(self):
        with open('kafka_config.yaml', 'r') as file:
            self.config = yaml.safe_load(file)

        self.brokers = ",".join(
            [f"{broker['hostname']}:{broker['port']}" for broker in self.config['kafka']['brokers']]
        )


# TODO: Test
class KafkaProduceHandler(KafkaHandler):
    def __init__(self):
        super().__init__()

        conf = {
            'bootstrap.servers': self.brokers,
            'transactional.id': self.config['kafka']['producer']['transactional_id'],
            'acks': self.config['kafka']['producer']['acks'],
            'enable.idempotence': self.config['kafka']['producer']['enable_idempotence']
        }

        try:
            self.producer = Producer(conf)
            self.producer.init_transactions()
        except KafkaError as e:
            logger.error(f"Producer initialization failed: {e}")
            raise

    def send(self, topic: str, data: str):
        try:
            self.producer.begin_transaction()

            self.producer.produce(
                topic=topic,
                key=None,  # could maybe add a key here
                value=data.encode('utf-8'),
                callback=self.kafka_delivery_report,
            )

            self.producer.commit_transaction()
        except KafkaException as e:
            self.producer.abort_transaction()
            logger.debug(f"Transaction failed: {e}")

        self.producer.flush()

    @staticmethod
    def kafka_delivery_report(err: None | KafkaError, msg: None | Message):
        if err:
            logger.warning('Message delivery failed: {}'.format(err))
        else:
            logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


# TODO: Test
class KafkaConsumeHandler(KafkaHandler):
    def __init__(self, topics):
        super().__init__()

        conf = {
            'bootstrap.servers': self.brokers,
            'group.id': self.config['kafka']['consumer']['group_id'],
            'enable.auto.commit': self.config['kafka']['consumer']['enable_auto_commit'],
            'isolation.level': self.config['kafka']['consumer']['isolation_level']
        }

        try:
            self.consumer = Consumer(conf)
            self.consumer.subscribe(topics)
        except KafkaError as e:
            logger.error(f"Consumer initialization failed: {e}")
            raise

    def __del__(self):
        self.consumer.close()

    def receive(self) -> str:
        message = self.consumer.poll(timeout=1.0)

        if not message:
            raise KafkaMessageFetchException("No message fetched from Kafka broker.")

        if message.error():
            raise IOError(message.error())

        return message.value().decode('utf-8')
