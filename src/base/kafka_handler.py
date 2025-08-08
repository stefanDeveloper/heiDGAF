"""
The Write-Exactly-Once-Semantics used by the :class:`KafkaHandler` is shown by
https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/eos-transactions.py,
parts of which are similar to the code in this module.
"""

import ast
import json
import os
import sys
import time
import uuid
from abc import abstractmethod

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
import uuid

logger = get_logger()

HOSTNAME = os.getenv("HOSTNAME", "default_tid")
CONSUMER_GROUP_ID = os.getenv("GROUP_ID", "default_gid")
NUMBER_OF_INSTANCES = int(os.getenv("NUMBER_OF_INSTANCES", 1))

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


class KafkaProduceHandler(KafkaHandler):
    """
    Base class for Kafka Producer wrappers.
    """

    def __init__(self, conf):
        super().__init__()
        self.producer = Producer(conf)

    @abstractmethod
    def produce(self, *args, **kwargs):
        """
        Encodes the given data for transport and sends it on the specified topic.
        """
        raise NotImplementedError

    def __del__(self) -> None:
        self.producer.flush()


class SimpleKafkaProduceHandler(KafkaProduceHandler):
    """
    Simple wrapper for the Kafka Producer without Write-Exactly-Once semantics.
    """

    def __init__(self):
        self.brokers = ",".join(
            [f"{broker['hostname']}:{broker['port']}" for broker in KAFKA_BROKERS]
        )

        conf = {
            "bootstrap.servers": self.brokers,
            "enable.idempotence": False,
            "acks": "1",
            "message.max.bytes": 1000000000,
        }

        super().__init__(conf)

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
        self.producer.flush()
        self.producer.produce(
            topic=topic,
            key=key,
            value=data,
            callback=kafka_delivery_report,
        )


class ExactlyOnceKafkaProduceHandler(KafkaProduceHandler):
    """
    Wrapper for the Kafka Producer with Write-Exactly-Once semantics.
    """

    def __init__(self):
        self.brokers = ",".join(
            [f"{broker['hostname']}:{broker['port']}" for broker in KAFKA_BROKERS]
        )

        conf = {
            "bootstrap.servers": self.brokers,
            "transactional.id": f"{HOSTNAME}-{uuid.uuid4()}",
            "enable.idempotence": True,
            "message.max.bytes": 1000000000,
        }

        super().__init__(conf)
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
        except Exception as e:
            logger.info(f"aborted for topic {topic}")
            self.producer.abort_transaction()
            logger.error("Transaction aborted.")
            logger.error(e)
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

        # get brokers
        self.brokers = ",".join(
            [f"{broker['hostname']}:{broker['port']}" for broker in KAFKA_BROKERS]
        )

        # create consumer
        conf = {
            "bootstrap.servers": self.brokers,
            "group.id": f"{CONSUMER_GROUP_ID}",
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
        """
        Consumes available messages on the specified topic and decodes it.
        """
        raise NotImplementedError

    def consume_as_json(self) -> tuple[None | str, dict]:
        """
        Consumes available messages on the specified topic. Decodes the data and returns the contents in JSON format.
        Blocks and waits if no data is available.

        Returns:
            Consumed data in JSON format

        Raises:
            ValueError: Invalid data format
        """
        key, value, topic = self.consume()

        if not key and not value:
            return None, {}

        try:
            eval_data = json.loads(value)

            if isinstance(eval_data, dict):
                return key, eval_data
            else:
                raise
        except Exception:
            raise ValueError("Unknown data format")

    def _all_topics_created(self, topics) -> bool:
        """
        Checks whether each topic in a list of topics was created. If not, retries for a set amount of times

        Args:
            topics (list): List of topics to check
        Returns:
            bool
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
        if self.consumer:
            self.consumer.close()

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
        """
        key, value, topic = self.consume()
        if not key and not value:
            # TODO: Change return value to fit the type, maybe switch to raise
            return None, {}
        eval_data: dict = json.loads(value)
        if self._is_dicts(eval_data.get("data")):
            eval_data["data"] = eval_data.get("data")
        else:
            eval_data["data"] = [json.loads(item) for item in eval_data.get("data")]
        batch_schema = marshmallow_dataclass.class_schema(Batch)()
        eval_data: Batch = batch_schema.load(eval_data)
        if isinstance(eval_data, Batch):
            return key, eval_data
        else:
            raise ValueError("Unknown data format.")


class SimpleKafkaConsumeHandler(KafkaConsumeHandler):
    """
    Simple wrapper for the Kafka Consumer without Write-Exactly-Once semantics.
    """

    def __init__(self, topics):
        super().__init__(topics)

    def consume(self) -> tuple[str | None, str | None, str | None]:
        """
        Consumes available messages on the specified topic. Decodes the data and returns a tuple
        of key, data and topic of the message. Blocks and waits if no data is available.

        Returns:
            Either ``[None,None,None]`` if empty data was retrieved or ``[key,value,topic]`` as tuple
            of strings of the consumed data.
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
    """
    Wrapper for the Kafka Consumer with Write-Exactly-Once semantics.
    """

    def __init__(self, topics: str | list[str]) -> None:
        super().__init__(topics)

    def consume(self) -> tuple[str | None, str | None, str | None]:
        """
        Consumes available messages on the specified topic. Decodes the data and returns a tuple
        of key, data and topic of the message. Blocks and waits if no data is available.

        Returns:
            Either ``[None,None,None]`` if empty data was retrieved or ``[key,value,topic]`` as tuple
            of strings of the consumed data.
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
