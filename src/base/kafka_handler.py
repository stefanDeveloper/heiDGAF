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
from typing import Optional

import marshmallow_dataclass
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
)
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.append(os.getcwd())
from src.base.data_classes.batch import Batch
from src.base.log_config import get_logger
from src.base.utils import kafka_delivery_report, setup_config

logger = get_logger()

HOSTNAME = os.getenv("HOSTNAME", "default_tid")
CONSUMER_GROUP_ID = os.getenv("GROUP_ID", "default_gid")
NUMBER_OF_INSTANCES = int(os.getenv("NUMBER_OF_INSTANCES", 1))

config = setup_config()
KAFKA_BROKERS = config["environment"]["kafka_brokers"]


class TooManyFailedAttemptsError(Exception):
    """Exception raised when operations exceed the maximum number of retry attempts

    This exception is typically raised during Kafka topic creation or connection
    establishment when the maximum number of retry attempts has been exceeded.
    """

    pass


class KafkaMessageFetchException(Exception):
    """Exception raised when Kafka message consumption fails

    This exception is raised when there are errors during the process of fetching
    or consuming messages from Kafka topics, including network issues, timeout
    errors, or malformed message data.
    """

    pass


class KafkaHandler:
    """Base class for all Kafka wrappers and handlers

    Provides common initialization and configuration setup for Kafka producers
    and consumers. This abstract base class establishes the foundation for
    specific Kafka handling implementations.
    """

    def __init__(self) -> None:
        """
        Sets up the initial configuration and initializes the consumer attribute
        to None. Specific implementations should override this method to establish
        their respective Kafka clients.
        """
        self.consumer = None


class KafkaProduceHandler(KafkaHandler):
    """Abstract base class for Kafka Producer wrappers

    Extends KafkaHandler to provide producer-specific functionality. This class
    establishes the interface for Kafka message production with different
    semantic guarantees (simple vs exactly-once).
    """

    def __init__(self, conf):
        """
        Args:
            conf (dict): Configuration dictionary for the Kafka producer.
                         Should contain broker settings and producer-specific options.
        """
        super().__init__()
        self.producer = Producer(conf)

    @abstractmethod
    def produce(self, *args, **kwargs):
        """Abstract method for producing messages to Kafka topics

        Encodes the given data for transport and sends it to the specified topic.
        Implementations must define the specific behavior for message production.

        Args:
            *args: Variable arguments depending on implementation.
            **kwargs: Keyword arguments depending on implementation.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError

    def __del__(self) -> None:
        """Cleanup method called when the object is destroyed

        Ensures that all pending messages are flushed before the producer
        is destroyed, preventing message loss.
        """
        self.producer.flush()


class SimpleKafkaProduceHandler(KafkaProduceHandler):
    """Simple Kafka Producer wrapper without Write-Exactly-Once semantics

    Provides basic message production capabilities with at-least-once delivery
    guarantees. This implementation prioritizes simplicity and performance over
    strict consistency guarantees.
    """

    def __init__(self):
        """
        Sets up a Kafka producer with standard configuration for simple message
        production without transactional guarantees. Broker addresses are
        automatically configured from the global KAFKA_BROKERS setting.
        """
        self.brokers = ",".join(
            [f"{broker['hostname']}:{broker['port']}" for broker in KAFKA_BROKERS]
        )

        conf = {
            "bootstrap.servers": self.brokers,
            "enable.idempotence": False,
            "acks": "1",
        }

        super().__init__(conf)

    def produce(self, topic: str, data: str, key: None | str = None) -> None:
        """Produce a message to the specified Kafka topic.

        Encodes and sends the provided data to the specified topic. The producer
        is flushed before sending to ensure message delivery. Empty data is
        silently ignored.

        Args:
            topic (str): Target Kafka topic name.
            data (str): Message data to send (ignored if empty).
            key (str, optional): Optional message key for partitioning.
                                 Default: None.

        Raises:
            KafkaException: If message production fails.
            BufferError: If the producer's message buffer is full.
        """
        if not data:
            return

        self.producer.flush()
        self.producer.produce(
            topic=topic,
            key=key,
            value=data,
            callback=kafka_delivery_report,
        )


class ExactlyOnceKafkaProduceHandler(KafkaProduceHandler):
    """Kafka Producer wrapper with Write-Exactly-Once semantics

    Provides transactional message production with exactly-once delivery
    guarantees. This implementation ensures that messages are delivered
    exactly once, even in the presence of failures and retries.

    Configuration:
        - transactional.id: Set to HOSTNAME for unique transaction identification
        - enable.idempotence: True (required for exactly-once semantics)

    Note:
        Each instance must have a unique transactional.id to avoid conflicts.
    """

    def __init__(self):
        """
        Sets up a Kafka producer with transactional capabilities for exactly-once
        semantics. The producer is initialized with transactions enabled and
        configured with a unique transactional ID based on the hostname.

        Raises:
            KafkaException: If transaction initialization fails.
        """
        self.brokers = ",".join(
            [f"{broker['hostname']}:{broker['port']}" for broker in KAFKA_BROKERS]
        )

        conf = {
            "bootstrap.servers": self.brokers,
            "transactional.id": HOSTNAME,
            "enable.idempotence": True,
        }

        super().__init__(conf)
        self.producer.init_transactions()

    def produce(self, topic: str, data: str, key: None | str = None) -> None:
        """Produce a message to the specified Kafka topic with exactly-once semantics.

        Sends the provided data within a Kafka transaction to ensure exactly-once
        delivery. The transaction is automatically committed on success or aborted
        on failure. Empty data is silently ignored.

        Args:
            topic (str): Target Kafka topic name.
            data (str): Message data to send (ignored if empty).
            key (str, optional): Optional message key for partitioning.
                                 Default: None.

        Raises:
            KafkaException: If message production or transaction handling fails.
            RuntimeError: If transaction commit fails after retries.
        """
        if not data:
            return

        self.producer.flush()
        self.producer.begin_transaction()

        try:
            self.producer.produce(
                topic=topic,
                key=key,
                value=data,
                callback=kafka_delivery_report,
            )

            self.commit_transaction_with_retry()
        except Exception:
            self.producer.abort_transaction()
            logger.error("Transaction aborted.")
            raise

    def commit_transaction_with_retry(
        self, max_retries: int = 3, retry_interval_ms: int = 1000
    ) -> None:
        """Commit a Kafka transaction with automatic retry logic.

        Attempts to commit the current transaction with built-in retry mechanism
        for handling transient failures. If committing fails due to conflicting
        API calls, the method will retry after the specified interval.

        Args:
            max_retries (int): Maximum number of commit retry attempts. Default: 3.
            retry_interval_ms (int): Time to wait between retries in milliseconds.
                                     Default: 1000.

        Raises:
            KafkaException: If transaction commit fails for reasons other than
                            conflicting API calls.
            RuntimeError: If transaction commit fails after all retry attempts.
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
    """Abstract base class for Kafka Consumer wrappers

    Provides common functionality for Kafka message consumption including
    topic creation, subscription management, and consumer configuration.
    All consumer implementations should extend this class.

    Attributes:
        brokers (str): Comma-separated list of Kafka broker addresses.
        consumer (Consumer): Confluent Kafka Consumer instance.
    """

    def __init__(self, topics: str | list[str]) -> None:
        """
        Creates a Kafka consumer, ensures the specified topics exist, and
        subscribes to them. Topics are automatically created if they don't exist.

        Args:
            topics (str | list[str]): Topic name(s) to subscribe to.
                                      Can be a single topic string or list of topics.

        Raises:
            TooManyFailedAttemptsError: If topic creation fails after retries.
            KafkaException: If consumer creation or subscription fails.
        """
        super().__init__()

        # get brokers
        self.brokers = ",".join(
            [f"{broker['hostname']}:{broker['port']}" for broker in KAFKA_BROKERS]
        )

        # create consumer
        conf = {
            "bootstrap.servers": self.brokers,
            "group.id": CONSUMER_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }
        self.consumer = Consumer(conf)

        if isinstance(topics, str):
            topics = [topics]

        # create topics
        admin_client = AdminClient(
            {
                "bootstrap.servers": self.brokers,
            }
        )
        admin_client.create_topics(
            [NewTopic(topic, NUMBER_OF_INSTANCES, 1) for topic in topics]
        )

        # check if topics are created
        if not self._all_topics_created(topics):
            raise TooManyFailedAttemptsError("Not all topics were created.")

        # subscribe to the topics
        self.consumer.subscribe(topics)

    @abstractmethod
    def consume(self, *args, **kwargs):
        """Abstract method for consuming messages from Kafka topics

        Implementations must define the specific behavior for message consumption,
        including how to handle message polling, error handling, and data decoding.

        Args:
            *args: Variable arguments depending on implementation.
            **kwargs: Keyword arguments depending on implementation.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError

    def consume_as_json(self) -> tuple[None | str, dict]:
        """Consume messages and return them in JSON format.

        Consumes available messages from subscribed topics, decodes the data,
        and returns the contents as a JSON dictionary. This method blocks
        until a message is available.

        Returns:
            tuple[None | str, dict]: A tuple containing:
                - Message key (str or None)
                - Message value as dictionary (empty dict if no message)

        Raises:
            ValueError: If the message data format is invalid or cannot be parsed.
        """
        key, value, topic = self.consume()

        if not key and not value:
            return None, {}

        try:
            eval_data = ast.literal_eval(value)

            if isinstance(eval_data, dict):
                return key, eval_data
            else:
                raise
        except Exception:
            raise ValueError("Unknown data format")

    def _all_topics_created(self, topics: list[str]) -> bool:
        """Verify that all specified topics have been created successfully.

        Polls the Kafka cluster to check if each topic in the provided list
        has been created. Retries for a maximum duration if topics are not
        immediately available.

        Args:
            topics (list[str]): List of topic names to verify.

        Returns:
            bool: True if all topics are created, False if timeout exceeded.
        """
        number_of_retries_left = 30
        all_topics_created = False
        while not all_topics_created:  # try for 15 seconds
            assigned_topics = self.consumer.list_topics(timeout=10)

            all_topics_created = True
            for topic in topics:
                if topic not in assigned_topics.topics:
                    all_topics_created = False

            if not all_topics_created:
                number_of_retries_left -= 1

            if not number_of_retries_left > 0:
                return False

            time.sleep(0.5)

        return True

    def __del__(self) -> None:
        """Cleanup method called when the object is destroyed

        Properly closes the Kafka consumer connection to release resources
        and ensure graceful shutdown.
        """
        if self.consumer:
            self.consumer.close()


class SimpleKafkaConsumeHandler(KafkaConsumeHandler):
    """Simple Kafka Consumer wrapper without Write-Exactly-Once semantics

    Provides basic message consumption capabilities with at-least-once delivery
    semantics. Messages are not automatically committed, allowing for manual
    offset management by the application.
    """

    def __init__(self, topics: str | list[str]) -> None:
        """
        Args:
            topics (str | list[str]): Topic name(s) to subscribe to.
        """
        super().__init__(topics)

    def consume(self) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Consume messages from subscribed Kafka topics.

        Polls for available messages and decodes them. This method blocks
        until a message is available or a keyboard interrupt is received.
        The consumer does not automatically commit offsets.

        Returns:
            tuple[Optional[str], Optional[str], Optional[str]]: A tuple containing:
                - Message key (str or None)
                - Message value (str or None)
                - Topic name (str or None)
                Returns (None, None, None) if no valid message is retrieved.

        Raises:
            ValueError: If the received message is invalid.
            KeyboardInterrupt: If consumption is interrupted by user.
            KafkaException: If message commit fails.
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
                        raise ValueError("Message is invalid")

                # unpack message
                key = msg.key().decode("utf-8") if msg.key() else None
                value = msg.value().decode("utf-8") if msg.value() else None
                topic = msg.topic() if msg.topic() else None

                return key, value, topic
        except KeyboardInterrupt:
            logger.info("Stopping KafkaConsumeHandler...")


class ExactlyOnceKafkaConsumeHandler(KafkaConsumeHandler):
    """Kafka Consumer wrapper with Write-Exactly-Once semantics

    Provides message consumption with exactly-once processing guarantees.
    Messages are automatically committed after successful processing to
    ensure each message is processed exactly once.
    """

    def __init__(self, topics: str | list[str]) -> None:
        """
        Args:
            topics (str | list[str]): Topic name(s) to subscribe to.
        """
        super().__init__(topics)

    def consume(self) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Consume messages from subscribed Kafka topics with exactly-once semantics.

        Polls for available messages, decodes them, and automatically commits
        the message offset after successful processing. This ensures each
        message is processed exactly once.

        Returns:
            tuple[Optional[str], Optional[str], Optional[str]]: A tuple containing:
                - Message key (str or None)
                - Message value (str or None)
                - Topic name (str or None)
                Returns (None, None, None) if no valid message is retrieved.

        Raises:
            ValueError: If the received message is invalid.
            KeyboardInterrupt: If consumption is interrupted by user.
            KafkaException: If message commit fails.
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
                        raise ValueError("Message is invalid")

                # unpack message
                key = msg.key().decode("utf-8") if msg.key() else None
                value = msg.value().decode("utf-8") if msg.value() else None
                topic = msg.topic() if msg.topic() else None

                self.consumer.commit(msg)

                return key, value, topic
        except KeyboardInterrupt:
            logger.info("Shutting down KafkaConsumeHandler...")

    @staticmethod
    def _is_dicts(obj):
        """Check if the provided object is a list containing only dictionaries.

        Args:
            obj: Object to check.

        Returns:
            bool: True if obj is a list of dictionaries, False otherwise.
        """
        return isinstance(obj, list) and all(isinstance(item, dict) for item in obj)

    def consume_as_object(self) -> tuple[Optional[str], Batch]:
        """
        Consume messages and return them as Batch objects.

        Consumes available messages from subscribed topics, decodes the data,
        and converts it to a structured Batch object using marshmallow schema
        validation. This method provides type-safe message consumption.

        Returns:
            tuple[Optional[str], Batch]: A tuple containing:
                - Message key (str or None).
                - Batch object containing the deserialized message data.

        Raises:
            ValueError: If the message data format is invalid or cannot be
                        converted to a Batch object.
            marshmallow.ValidationError: If data doesn't conform to Batch schema.
        """
        key, value, topic = self.consume()

        if not key and not value:
            # TODO: Change return value to fit the type, maybe switch to raise
            return None, {}

        eval_data: dict = ast.literal_eval(value)

        if self._is_dicts(eval_data.get("data")):
            eval_data["data"] = eval_data.get("data")
        else:
            eval_data["data"] = [
                ast.literal_eval(item) for item in eval_data.get("data")
            ]

        batch_schema = marshmallow_dataclass.class_schema(Batch)()
        eval_data: Batch = batch_schema.load(eval_data)

        if isinstance(eval_data, Batch):
            return key, eval_data
        else:
            raise ValueError("Unknown data format.")
