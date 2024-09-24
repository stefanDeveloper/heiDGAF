"""
The Write-Exactly-Once-Semantics used by the :class:`KafkaHandler` is shown by
https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/eos-transactions.py,
parts of which are similar to the code in this module.
"""

import ast
import json
import logging
import os
import sys
import time

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
import marshmallow_dataclass

sys.path.append(os.getcwd())
from src.base import Batch
from src.base.log_config import setup_logging
from src.base.utils import kafka_delivery_report, setup_config

setup_logging()
logger = logging.getLogger(__name__)

config = setup_config()
KAFKA_BROKERS = config["kafka"]["brokers"]
CONSUMER_GROUP_ID = config["kafka"]["consumer"]["group_id"]


class TooManyFailedAttemptsError(Exception):
    """
    Exception for too many failed attempts.
    """

    pass


class KafkaMessageFetchException(Exception):
    """
    Exception for failed fetch of Kafka messages during consuming.
    """

    pass


class KafkaHandler:
    """
    Wraps and adds up on the Kafka functionality. :class:`KafkaHandler` serves as base class for further implementation
    in its inheriting classes. Base class only specifies the initialization.
    """

    def __init__(self) -> None:
        """
        Initializes the brokers used in further tasks.
        """
        logger.debug(f"Initializing KafkaHandler...")
        self.consumer = None

        self.brokers = ",".join(
            [f"{broker['hostname']}:{broker['port']}" for broker in KAFKA_BROKERS]
        )
        logger.debug(f"Retrieved {self.brokers=}.")
        logger.debug(f"Initialized KafkaHandler.")


class KafkaProduceHandler(KafkaHandler):
    """
    Wraps the Kafka Producer functionality of producing data into the Broker(s) using a topic and a
    ``transactional_id`` specified in initialization. Also uses the Write-Exactly-Once-Semantics which requires
    handling and committing transactions. The topic and data are specified in the method call of :meth:`send()`.
    """

    def __init__(self, transactional_id: str):
        """
        Args:
            transactional_id (str): ID of the transaction

        Raises:
            KafkaException: During initialization of Producer or its transactions
        """
        logger.debug(f"Initializing KafkaProduceHandler ({transactional_id=})...")
        super().__init__()

        self.batch_schema = marshmallow_dataclass.class_schema(Batch)()

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
        except KafkaException as e:
            logger.error(f"Producer initialization failed: {e}")
            raise

        logger.debug(f"Initialized KafkaProduceHandler ({transactional_id=}).")

    def __del__(self) -> None:
        """
        Flushes the producer to securely delete the instance.
        """
        logger.debug("Closing KafkaProduceHandler...")
        self.producer.flush()
        logger.debug("Closed KafkaProduceHandler.")

    def send(self, topic: str, data: str, key: None | str) -> None:
        """
        Encodes the given data for transport and sends it with the specified topic.

        Args:
            topic (str): Topic to send the data with.
            data (str): Data to be sent.
            key (str): Key to send the data with.

        Raises:
            Exception: During :meth:`commit_transaction_with_retry()` or Producer's ``produce()``. Aborts
                       transaction then.
        """
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
                key=key,
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

    def commit_transaction_with_retry(
        self, max_retries: int = 3, retry_interval_ms: int = 1000
    ) -> None:
        """
        Commits a transaction including retries. If committing fails, it is retried after the given retry interval
        time up to ``max_retries`` times.

        Args:
            max_retries (int): Maximum number of retries
            retry_interval_ms (int): Interval between retries in ms
        """
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


class KafkaConsumeHandler(KafkaHandler):
    """
    Wraps and adds up on the Kafka Consumer functionality of consuming data from the Broker(s) in a specified topic.
    Also uses the Write-Exactly-Once-Semantics which requires handling and committing transactions.
    """

    def __init__(self, topic: str) -> None:
        """
        Args:
            topic (str): Topic name to consume from

        Raises:
            KafkaException: During construction of Consumer or assignment of topic.
        """
        logger.debug(f"Initializing KafkaConsumeHandler ({topic=})...")
        super().__init__()

        conf = {
            "bootstrap.servers": self.brokers,
            "group.id": CONSUMER_GROUP_ID,
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
        except KafkaException as e:
            logger.error(f"Consumer initialization failed: {e}")
            raise e

        logger.debug(f"Initialized KafkaConsumeHandler ({topic=}).")

    def __del__(self) -> None:
        """
        Deletes the instance. Closes the running Kafka Consumer if it exists.
        """
        logger.debug("Deleting KafkaConsumeHandler...")
        if self.consumer:
            self.consumer.close()
        logger.debug("KafkaConsumeHandler deleted.")

    def consume(self) -> tuple[str | None, str | None]:
        """
        Consumes available messages from the Broker(s) in the specified topic. Decodes the data and returns a tuple
        of key and data of the message. Blocks and waits if no data is available.

        Returns:
            Either ``[None,None]`` if empty data was retrieved from the Broker(s) or ``[key,value]`` as tuple
            of strings of the consumed data.

        Raises:
            KeyboardInterrupt: Execution interrupted by user
            Exception: Error during consuming
        """
        logger.debug("Starting to consume messages...")

        empty_data_retrieved = False
        try:
            while True:
                logger.debug("Polling available messages...")
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    if not empty_data_retrieved:
                        logger.info("Waiting for messages to be produced...")

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
                logger.debug(f"Received message: {key=}, {value=}")
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
            raise

    def consume_and_return_json_data(self) -> tuple[None | str, dict]:
        """
        Calls the :meth:`consume()` method and waits for it to return data. Loads the data and converts it to a JSON
        object. Returns the JSON data.

        Returns:
            Consumed data in JSON format

        Raises:
            ValueError: Invalid data format
            KafkaMessageFetchException: Error during message fetching/consuming
            KeyboardInterrupt: Execution interrupted by user
        """
        try:
            key, value = self.consume()

            if not key and not value:
                logger.debug("No data returned.")
                return None, {}
        except KafkaMessageFetchException as e:
            logger.debug(e)
            raise
        except KeyboardInterrupt:
            raise

        logger.debug("Loading JSON values from received data...")
        json_from_message = json.loads(value)
        logger.debug(f"{json_from_message=}")
        eval_data = ast.literal_eval(value)

        if isinstance(eval_data, dict):
            logger.debug("Loaded available data. Returning it...")
            return key, eval_data
        else:
            logger.error("Unknown data format.")
            raise ValueError

    def consume_and_return_object(self) -> tuple[None | str, Batch]:
        """
        Calls the :meth:`consume()` method and waits for it to return data. Loads the data and converts it to a Batch
        object. Returns the Batch object.

        Returns:
            Consumed data in Batch object

        Raises:
            ValueError: Invalid data format
            KafkaMessageFetchException: Error during message fetching/consuming
            KeyboardInterrupt: Execution interrupted by user
        """
        try:
            key, value = self.consume()

            if not key and not value:
                logger.debug("No data returned.")
                return None, {}
        except KafkaMessageFetchException as e:
            logger.debug(e)
            raise
        except KeyboardInterrupt:
            raise

        logger.debug("Loading JSON values from received data...")
        json_from_message = json.loads(value)
        logger.debug(f"{json_from_message=}")
        eval_data = self.batch_schema.loads(value)

        if isinstance(eval_data, dict):
            logger.debug("Loaded available data. Returning it...")
            return key, eval_data
        else:
            logger.error("Unknown data format.")
            raise ValueError
