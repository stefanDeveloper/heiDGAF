# Inspired by https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/eos-transactions.py
import ast
import json
import logging
import os  # needed for Terminal execution
import sys  # needed for Terminal execution
import time

import yaml
from confluent_kafka import KafkaError, Producer, Consumer, TopicPartition, KafkaException

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


class KafkaProduceHandler(KafkaHandler):
    def __init__(self, transactional_id: str):
        super().__init__()

        conf = {
            'bootstrap.servers': self.brokers,
            'transactional.id': transactional_id,
        }

        try:
            self.producer = Producer(conf)
            self.producer.init_transactions()
        except KafkaError as e:
            logger.error(f"Producer initialization failed: {e}")
            raise

    def send(self, topic: str, data: str):
        if not data:
            logger.warning("No data! Not sending any data.")
            return

        self.producer.begin_transaction()
        try:
            self.producer.produce(
                topic=topic,
                key=None,  # could maybe add a key here
                value=data.encode('utf-8'),
                callback=kafka_delivery_report,
            )
            self.commit_transaction_with_retry()
            logger.debug(f"Transaction for {data=} committed.")
        except Exception as e:
            logger.warning(f"Transaction for {data=} failed: {e}")
            self.producer.abort_transaction()

    def commit_transaction_with_retry(self, max_retries=3, retry_interval_ms=1000):
        committed = False
        retry_count = 0

        while not committed and retry_count < max_retries:
            try:
                self.producer.commit_transaction()
                committed = True
            except KafkaException as e:
                if "Conflicting commit_transaction API call is already in progress" in str(e):
                    retry_count += 1
                    logger.debug("Conflicting commit_transaction API call is already in progress: Retrying")
                    time.sleep(retry_interval_ms / 1000.0)
                else:
                    raise e

        if not committed:
            raise RuntimeError("Failed to commit transaction after retries")

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

    def consume_and_return_json_data(self) -> list:
        try:
            key, value = self.consume()

            if not key and not value:
                logger.debug("No data returned.")
                return []
        except KafkaMessageFetchException as e:
            logger.debug(e)
            raise
        except KeyboardInterrupt:
            raise
        except IOError as e:
            logger.error(e)
            raise

        json_from_message = json.loads(value)
        json_data = []

        for e in json_from_message:
            json_data.append(ast.literal_eval(e))

        return json_data


if __name__ == '__main__':
    producer_handler = KafkaProduceHandler("test_id")
    producer_handler.send("test", "Test")

    consumer_handler = KafkaConsumeHandler("test")
    consumer_handler.consume()
