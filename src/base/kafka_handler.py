"""
The Write-Exactly-Once-Semantics used by the :class:`KafkaHandler` is shown by
https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/eos-transactions.py,
parts of which are similar to the code in this module.
"""

import ast
import os
import sys
import time
from abc import abstractmethod

import marshmallow_dataclass
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)

sys.path.append(os.getcwd())
from src.base import Batch
from src.base.log_config import get_logger
from src.base.utils import kafka_delivery_report, setup_config

logger = get_logger()

CONSUMER_GROUP_ID = "consumer_group"

config = setup_config()
KAFKA_BROKERS = config["environment"]["kafka_brokers"]


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
    Base class for all Kafka wrappers. Only specifies the initialization.
    """

    def __init__(self) -> None:
        """
        Initializes the broker configuration.
        """
        self.consumer = None
        self.brokers = ",".join(
            [f"{broker['hostname']}:{broker['port']}" for broker in KAFKA_BROKERS]
        )


class KafkaProduceHandler(KafkaHandler):
    """
    Base class for Kafka Producer wrappers.
    """

    def __init__(self, transactional_id: str):
        super().__init__()

        conf = {
            "bootstrap.servers": self.brokers,
            "transactional.id": transactional_id,
        }

        self.producer = Producer(conf)

    @abstractmethod
    def produce(self, *args, **kwargs):
        """
        Encodes the given data for transport and sends it on the specified topic.
        """
        pass

    def __del__(self) -> None:
        self.producer.flush()


class SimpleKafkaProduceHandler(KafkaProduceHandler):
    """
    Simple wrapper for the Kafka Producer without Write-Exactly-Once semantics.
    """

    def produce(self, topic: str, data: str, key: None | str = None) -> None:
        """
        Encodes the given data for transport and sends it on the specified topic.

        Args:
            topic (str): Topic to send the data with
            data (str): Data to be sent
            key (str): Key to send the data with
        """
        if not data:
            return

        self.producer.produce(
            topic=topic,
            key=key,
            value=data.encode("utf-8"),
            callback=kafka_delivery_report,
        )


class ExactlyOnceKafkaProduceHandler(KafkaProduceHandler):
    """
    Wrapper for the Kafka Producer with Write-Exactly-Once semantics.
    """

    def __init__(self, transactional_id: str):
        super().__init__(transactional_id)
        self.producer.init_transactions()

    def produce(self, topic: str, data: str, key: None | str = None) -> None:
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
        if not data:
            return

        self.producer.begin_transaction()
        try:
            self.producer.produce(
                topic=topic,
                key=key,
                value=data.encode("utf-8"),
                callback=kafka_delivery_report,
            )

            self.commit_transaction_with_retry()
        except Exception as e:
            self.producer.abort_transaction()
            logger.error("Transaction aborted.")
            raise

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
            raise RuntimeError("Failed to commit transaction after retries.")


class KafkaConsumeHandler(KafkaHandler):
    """
    Base class for Kafka Consumer wrappers.
    """

    def __init__(self, topics: str | list[str]) -> None:
        super().__init__()

        conf = {
            "bootstrap.servers": self.brokers,
            "group.id": CONSUMER_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }

        if isinstance(topics, str):
            topics = [topics]

        self.consumer = Consumer(conf)
        self.consumer.assign([TopicPartition(topic, 0) for topic in topics])

    @abstractmethod
    def consume(self, *args, **kwargs):
        """
        Consumes available messages on the specified topic and decodes it.
        """
        pass

    def consume_as_json(self) -> tuple[None | str, dict]:
        """
        Consumes available messages on the specified topic. Decodes the data and returns the contents in JSON format.
        Blocks and waits if no data is available.

        Returns:
            Consumed data in JSON format

        Raises:
            ValueError: Invalid data format
            KafkaMessageFetchException: Error during message fetching/consuming
            KeyboardInterrupt: Execution interrupted by user
        """
        try:
            key, value, topic = self.consume()

            if not key and not value:
                return None, {}
        except KafkaMessageFetchException:
            raise
        except KeyboardInterrupt:
            raise

        eval_data = ast.literal_eval(value)

        if isinstance(eval_data, dict):
            return key, eval_data
        else:
            raise ValueError("Unknown data format")

    def __del__(self) -> None:
        if self.consumer:
            self.consumer.close()


class SimpleKafkaConsumeHandler(KafkaConsumeHandler):
    """
    Simple wrapper for the Kafka Consumer without Write-Exactly-Once semantics.
    """

    def consume(self) -> tuple[str | None, str | None, str | None]:
        """
        Consumes available messages on the specified topic. Decodes the data and returns a tuple
        of key, data and topic of the message. Blocks and waits if no data is available.

        Returns:
            Either ``[None,None,None]`` if empty data was retrieved or ``[key,value,topic]`` as tuple
            of strings of the consumed data.

        Raises:
            KeyboardInterrupt: Execution interrupted by user
            Exception: Error during consuming
        """
        empty_data_retrieved = False

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    if not empty_data_retrieved:
                        logger.info("Waiting for messages...")

                    empty_data_retrieved = True
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        raise

                # unpack message
                key = msg.key().decode("utf-8") if msg.key() else None
                value = msg.value().decode("utf-8") if msg.value() else None
                topic = msg.topic() if msg.topic() else None

                return key, value, topic
        except KeyboardInterrupt:
            logger.info("Stopping KafkaConsumeHandler...")
            raise KeyboardInterrupt
        except Exception as e:
            raise


class ExactlyOnceKafkaConsumeHandler(KafkaConsumeHandler):
    """
    Wrapper for the Kafka Consumer with Write-Exactly-Once semantics.
    """

    def __init__(self, topics: str | list[str]) -> None:
        self.batch_schema = marshmallow_dataclass.class_schema(Batch)()
        super().__init__(topics)

    def consume(self) -> tuple[str | None, str | None, str | None]:
        """
        Consumes available messages on the specified topic. Decodes the data and returns a tuple
        of key, data and topic of the message. Blocks and waits if no data is available.

        Returns:
            Either ``[None,None,None]`` if empty data was retrieved or ``[key,value,topic]`` as tuple
            of strings of the consumed data.

        Raises:
            KeyboardInterrupt: Execution interrupted by user
            Exception: Error during consuming
        """
        empty_data_retrieved = False

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    if not empty_data_retrieved:
                        logger.info("Waiting for messages...")

                    empty_data_retrieved = True
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        raise

                # unpack message
                key = msg.key().decode("utf-8") if msg.key() else None
                value = msg.value().decode("utf-8") if msg.value() else None
                topic = msg.topic() if msg.topic() else None

                self.consumer.commit(msg)

                return key, value, topic
        except KeyboardInterrupt:
            logger.info("Shutting down KafkaConsumeHandler...")
            raise KeyboardInterrupt
        except Exception as e:
            raise

    @staticmethod
    def _is_dicts(obj):
        return isinstance(obj, list) and all(isinstance(item, dict) for item in obj)

    def consume_as_object(self) -> tuple[None | str, Batch]:
        """
        Consumes available messages on the specified topic. Decodes the data and converts it to a Batch
        object. Returns the Batch object.

        Returns:
            Consumed data as Batch object

        Raises:
            ValueError: Invalid data format
            KafkaMessageFetchException: Error during message fetching/consuming
            KeyboardInterrupt: Execution interrupted by user
        """
        try:
            key, value, topic = self.consume()

            if not key and not value:
                return None, {}
        except KafkaMessageFetchException as e:
            logger.warning(e)
            raise
        except KeyboardInterrupt:
            raise

        eval_data: dict = ast.literal_eval(value)

        if self._is_dicts(eval_data.get("data")):
            eval_data["data"] = eval_data.get("data")
        else:
            eval_data["data"] = [
                ast.literal_eval(item) for item in eval_data.get("data")
            ]

        eval_data: Batch = self.batch_schema.load(eval_data)

        if isinstance(eval_data, Batch):
            return key, eval_data
        else:
            raise ValueError("Unknown data format.")
