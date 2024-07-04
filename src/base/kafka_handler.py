# Inspired by https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/eos-transactions.py
import ast
import json
import logging
import os  # needed for Terminal execution
import sys  # needed for Terminal execution
import time

import yaml
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)

sys.path.append(os.getcwd())  # needed for Terminal execution
from src.base.config import *
from src.base.log_config import setup_logging
from src.base.utils import kafka_delivery_report

setup_logging()
logger = logging.getLogger(__name__)


class TooManyFailedAttemptsError(Exception):
    pass


class KafkaMessageFetchException(Exception):
    pass


class KafkaHandler:
    def __init__(self):
        logger.debug(f"Initializing KafkaHandler...")
        self.consumer = None

        try:
            logger.debug(f"Opening configuration file at {CONFIG_FILEPATH}...")
            with open(CONFIG_FILEPATH, "r") as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            logger.critical(f"File {CONFIG_FILEPATH} not does not exist. Aborting...")
            raise
        logger.debug("Configuration file successfully opened and information stored.")

        self.brokers = ",".join(
            [
                f"{broker['hostname']}:{broker['port']}"
                for broker in self.config["kafka"]["brokers"]
            ]
        )
        logger.debug(f"Retrieved {self.brokers=}.")
        logger.debug(f"Initialized KafkaHandler.")


class KafkaProduceHandler(KafkaHandler):
    def __init__(self, transactional_id: str):
        logger.debug(f"Initializing KafkaProduceHandler ({transactional_id=})...")
        super().__init__()

        conf = {
            "bootstrap.servers": self.brokers,
            "transactional.id": transactional_id,
        }
        logger.debug(f"Set {conf=}.")

        try:
            logger.debug("Calling Producer(conf)...")
            self.producer = Producer(conf)
            logger.debug("Producer set. Initializing transactions...")
            self.producer.init_transactions()
            logger.debug("Transactions initialized.")
        except KafkaError as e:
            logger.error(f"Producer initialization failed: {e}")
            raise

        logger.debug(f"Initialized KafkaProduceHandler ({transactional_id=}).")

    def send(self, topic: str, data: str):
        logger.debug(f"Starting to send data to Producer...")
        logger.debug(f"({topic=}, {data=})")
        if not data:
            logger.debug("No data. Nothing to send. Returning...")
            return

        logger.debug("Beginning transaction...")
        self.producer.begin_transaction()
        logger.debug("Successfully began transaction.")
        try:
            logger.debug(f"Calling Producer for producing {topic=}, key=None...")
            self.producer.produce(
                topic=topic,
                key=None,  # could maybe add a key here
                value=data.encode("utf-8"),
                callback=kafka_delivery_report,
            )
            logger.debug(
                "Producer.produce() successfully called. Committing transaction..."
            )
            self.commit_transaction_with_retry()
            logger.debug(f"Transaction committed.")
            logger.debug(f"({data=})")
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            logger.error(f"({data=})")
            self.producer.abort_transaction()
            logger.error("Transaction aborted.")
            raise

        logger.debug("Data sent to Producer.")
        logger.debug(f"({data=})")

    def commit_transaction_with_retry(self, max_retries=3, retry_interval_ms=1000):
        logger.debug(
            f"Committing transaction with up to {max_retries} retries ({retry_interval_ms=})..."
        )
        committed = False
        retry_count = 0

        while not committed and retry_count < max_retries:
            try:
                self.producer.commit_transaction()
                committed = True
            except KafkaException as e:
                if (
                    "Conflicting commit_transaction API call is already in progress"
                    in str(e)
                ):
                    retry_count += 1
                    logger.debug(
                        "Conflicting commit_transaction API call is already in progress: Retrying"
                    )
                    time.sleep(retry_interval_ms / 1000.0)
                else:
                    raise e

        if not committed:
            logger.error("Transaction could not be committed.")
            raise RuntimeError("Failed to commit transaction after retries.")

        logger.debug(
            f"Successfully committed transaction after {retry_count} retry/retries."
        )

    def close(self):  # TODO: Change to __del__
        logger.debug("Closing KafkaProduceHandler...")
        self.producer.flush()
        logger.debug("Closed KafkaProduceHandler.")


# TODO: Test
class KafkaConsumeHandler(KafkaHandler):
    def __init__(self, topic: str):
        logger.debug(f"Initializing KafkaConsumeHandler ({topic=})...")
        super().__init__()

        conf = {
            "bootstrap.servers": self.brokers,
            "group.id": self.config["kafka"]["consumer"]["group_id"],
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }
        logger.debug(f"Set {conf=}.")

        try:
            logger.debug("Calling Consumer(conf)...")
            self.consumer = Consumer(conf)
            logger.debug(f"Consumer set. Assigning topic {topic}...")
            self.consumer.assign([TopicPartition(topic, 0)])
        except KafkaError as e:
            logger.error(f"Consumer initialization failed: {e}")
            raise

        logger.debug(f"Initialized KafkaConsumeHandler ({topic=}).")

    def __del__(self):
        logger.debug("Deleting KafkaConsumeHandler...")
        if self.consumer:
            self.consumer.close()
        logger.debug("KafkaConsumeHandler deleted.")

    def consume(self) -> tuple[str | None, str | None]:
        logger.debug("Starting to consume messages...")

        empty_data_retrieved = False
        try:
            while True:
                logger.debug("Polling available messages...")
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    if not empty_data_retrieved:
                        logger.info(
                            "No data to consume. Waiting for messages to be produced..."
                        )

                    empty_data_retrieved = True
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        raise

                key = msg.key().decode("utf-8") if msg.key() else None
                value = msg.value().decode("utf-8") if msg.value() else None
                logger.info(f"Received message: {key=}, {value=}")
                logger.debug("Committing transaction for message on Consumer...")
                self.consumer.commit(msg)
                logger.debug(
                    f"Transaction committed. Successfully consumed messages. Returning [{key=}, {value=}]..."
                )
                return key, value
        except KeyboardInterrupt:
            logger.info("Shutting down KafkaConsumeHandler...")
            raise KeyboardInterrupt
        except Exception as e:
            logger.error(f"Error in KafkaConsumeHandler: {e}")

    def consume_and_return_json_data(self) -> dict:
        try:
            key, value = self.consume()

            if not key and not value:
                logger.debug("No data returned.")
                return {}
        except KafkaMessageFetchException as e:
            logger.debug(e)
            raise
        except KeyboardInterrupt:
            raise
        except IOError as e:
            logger.error(e)
            raise

        logger.debug("Loading JSON values from received data...")
        json_from_message = json.loads(value)
        json_data = ast.literal_eval(json_from_message)
        logger.debug("Loaded available JSON data. Returning it...")

        return json_data
