import json
import logging
import os  # needed for Terminal execution
import sys  # needed for Terminal execution
from threading import Lock, Timer

from src.base.kafka_handler import KafkaProduceHandler
from src.base.utils import current_time, setup_config

sys.path.append(os.path.abspath('../..'))  # needed for Terminal execution
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

config = setup_config()
BATCH_SIZE = config["kafka"]["batch_sender"]["batch_size"]
BATCH_TIMEOUT = config["kafka"]["batch_sender"]["batch_timeout"]


class BufferedBatch:
    def __init__(self) -> None:
        self.batch = {}  # Batch for the latest messages coming in
        self.buffer = {}  # Former batch with previous messages

        self.begin_timestamps = {}
        self.center_timestamps = {}
        self.end_timestamps = {}

    def add_message(self, key: str, message: str) -> None:
        if key in self.batch:  # key already has messages associated
            self.batch[key].append(message)
            logger.debug(f"Message '{message}' added to {key}'s batch.")
        else:  # key has no messages associated yet
            self.batch[key] = [message]
            logger.debug(f"Message '{message}' added to newly created {key}'s batch.")

            self.center_timestamps[key] = current_time()
            logger.debug(f"center_timestamp of {key=} set to now: {self.center_timestamps[key]}")

    def complete_and_return_batch(self, key: str) -> dict:
        if self.buffer or self.batch:
            if not self.buffer:  # Variant 1: Only batch has entries
                logger.debug("Variant 1: Only batch has entries. Sending...")
                begin_timestamp = self.center_timestamps[key]
            else:  # Variant 2: Buffer and batch have entries
                logger.debug("Variant 2: Buffer and batch have entries. Sending...")
                begin_timestamp = self.begin_timestamps[key]

            end_timestamp = current_time()
            logger.debug(f"end_timestamp set to now: {end_timestamp}")
            data = {
                "begin_timestamp": begin_timestamp,
                "end_timestamp": end_timestamp,
                "data": self.buffer[key] + self.batch[key],
            }
            return data

        if self.buffer:  # Variant 3: Only buffer has entries
            logger.debug("Variant 3: Only buffer has entries.")
            logger.debug("Deleting buffer data (has no influence on analysis since it was too long ago)...")
        else:  # Variant 4: No data exists
            logger.debug("Variant 4: No data exists. Nothing to send.")

        return []


# TODO: Test
class MessageDictionary:
    """
    A data structure for storing and managing messages associated with unique keys, including time stamps.

    This class allows to associate a list of messages with each key, where a key is typically an IP address,
    but can be any string identifier. It provides methods to add messages, retrieve messages, and remove all
    messages associated with a specific key.
    """

    def __init__(self) -> None:
        self.key_dict = {}
        self.begin_timestamp = {}
        self.end_timestamp = {}

    def add_message(self, key: str, message: str) -> None:
        """
        Adds a message to the list of messages under the given key. Creates a new entry if the key doesn't exist.

        Args:
            message (str): Message to be stored.
            key (str): Key of the dictionary entry under which the message is stored.
        """
        if key in self.key_dict:
            self.key_dict[key].append(message)
            logger.debug(f"Message '{message}' added to {key}'s batch.")
        else:
            self.key_dict[key] = [message]
            logger.debug(f"Message '{message}' added to newly created {key}'s batch.")

            self.begin_timestamp[key] = current_time()
            logger.debug(f"begin_timestamp set to now: {self.begin_timestamp[key]}")

    def get_number_of_messages(self, key: str) -> int:
        """
        Returns the number of messages stored for this key.

        Args:
            key (str): Key of the dictionary for which to return the number of messages.

        Returns:
            Number of messages stored for this key as integer
        """
        return len(self.key_dict[key])

    def get_messages(self, key: str) -> list:
        """
        Retrieves messages for a specific key. Returns an empty list if the key has no messages.

        Returns:
            List of messages associated with the given key
        """
        return self.key_dict.get(key, [])

    def remove_messages(self, key: str) -> None:
        """
        Remove all messages and timestamps associated with a specific key.

        Args:
            key (str): Key for which to remove all messages, timestamps and the key itself.
        """
        if key in self.key_dict:
            del self.key_dict[key]
            del self.begin_timestamp[key]
            del self.end_timestamp[key]


class KafkaBatchSender:
    """
    Stores a batch of incoming data messages and sends the entire batch as single message to the respective
    :class:`KafkaHandler`. Batch is sent to the given topic when it is full or the timer runs out.
    """

    def __init__(self, topic: str, transactional_id: str) -> None:
        """
        The batch is kept until the next batch for this key reaches the maximum number of elements. Then,
        both batches are sent as concatenation. The older batch is removed and the same procedure repeats with
        the next batch.

        Args:
            topic (str): Topic to send the full batch with
            transactional_id (str): ID of the transaction to send the full batch with
        """
        logger.debug(
            f"Initializing KafkaBatchSender ({topic=}, {transactional_id=})..."
        )
        self.topic = topic
        self.latest_messages = MessageDictionary()
        self.earlier_messages = MessageDictionary()
        self.lock = Lock()
        self.timer = None
        self.begin_timestamp = None
        self.center_timestamp = None
        self.end_timestamp = None

        logger.debug(f"Calling KafkaProduceHandler({transactional_id=})...")
        self.kafka_produce_handler = KafkaProduceHandler(
            transactional_id=transactional_id
        )
        logger.debug(
            f"Initialized KafkaBatchSender ({topic=}, {transactional_id=})."
        )

    def add_message(self, key: str, message: str) -> None:
        """
        Adds the given message to the key specific batch. Checks if the batch is full. If so, it is sent. In the first
        execution, the timer starts. The same timer is used for all keys.

        Args:
            message (str): Message to be added to the batch
            key (str): Key of the message (e.g. client IP address in a log message)
        """
        logger.debug(f"Adding message '{message}' to batch.")
        with self.lock:
            self.latest_messages.add_message(key, message)

            if self.latest_messages.get_number_of_messages(key) >= BATCH_SIZE:
                logger.debug(f"Batch is full. Calling _send_batch({key=})...")
                self._send_batch_for_key(key)
            elif not self.timer:  # First time setting the timer
                logger.debug("Timer not set yet. Calling _reset_timer()...")
                self.begin_timestamp = current_time()
                logger.debug(f"begin_timestamp set to '{self.begin_timestamp}'")
                self._reset_timer()
        logger.debug(f"Message '{message}' successfully added to batch for {key=}.")

    def close(self):  # TODO: Change to __del__ and add docstring
        logger.debug(f"Closing KafkaBatchSender ({self.topic=})...")
        if self.timer:
            logger.debug("Timer is active. Cancelling timer...")
            self.timer.cancel()
            logger.debug("Timer cancelled.")

        for key in list(self.latest_messages.key_dict.keys()):
            logger.debug(f"Calling _send_batch({key=})...")
            self._send_batch_for_key(key)

        logger.debug(f"Closed KafkaBatchSender ({self.topic=}).")

    def _send_batch_for_key(self, key: str) -> None:
        """
        Sends the current batch for the given key if there is data. Also handles the storing of timestamps and the
        earlier batch. Before the first message of any key is added, the `begin_timestamp` is set to the current
        time. Before the batch is sent, an `end_timestamp` is set. Also resets the timer.

        More information on the handling of timestamps can be found here:
        https://heidgaf.readthedocs.io/en/latest/pipeline.html#timestamps-for-kafkabatchsender.
        """
        logger.debug(f"Starting to send the batch for {key=}...")

        # check if batches contain messages
        use_earlier_batch = True if self.earlier_messages.get_messages(key) else False
        use_latest_batch = True if self.latest_messages.get_messages(key) else False

        if use_earlier_batch or use_latest_batch:
            if not use_earlier_batch:  # Variant 1: Only new batch has entries
                logger.debug("Variant 1: Only new batch has entries. Sending...")
                data = self._get_new_batch_data_packet_to_send(key)
            else:  # Variant 2: Both batches have entries
                logger.debug("Variant 2: Both batches have entries. Sending...")
                data = self._get_full_data_packet_to_send(key)

            end_timestamp = data["end_timestamp"]
            self._send_data_packet(key, data)
            self._move_to_buffer(end_timestamp)

        if use_earlier_batch:  # Variant 3: Only old batch has entries
            logger.debug("Variant 3: Only old batch has entries.")
            logger.debug("Deleting old batch (has no influence on analysis since it was too long ago)...")
        else:  # Variant 4: No data exists
            logger.debug("Variant 4: No data exists. Nothing to send.")

        logger.debug("Calling _reset_timer()...")
        self._reset_timer()

    def _old_send_batch_for_key(self, key: str) -> None:
        """
        Sends the current batch for the given key if there is data. Also handles the storing of timestamps and the
        earlier batch. Before the first message of any key is added, the `begin_timestamp` is set to the current
        time. Before the batch is sent, an `end_timestamp` is set. Also resets the timer.

        More information on the handling of timestamps can be found here:
        https://heidgaf.readthedocs.io/en/latest/pipeline.html#timestamps-for-kafkabatchsender.
        """
        logger.debug("Starting to send the batch...")

        if self.earlier_messages or self.latest_messages:
            logger.debug(
                "Messages not empty. Trying to send batch to KafkaProduceHandler..."
            )

            if not self.buffer:
                self.begin_timestamp = self.end_timestamp
                logger.debug(f"begin_timestamp set to former end_timestamp: {self.begin_timestamp}")

            self.end_timestamp = current_time()
            logger.debug(f"end_timestamp set to now: {self.end_timestamp}")

            data_to_send = {
                "begin_timestamp": self.begin_timestamp,
                "end_timestamp": self.end_timestamp,
                "data": self.earlier_messages + self.latest_messages,
            }
            logger.debug(f"{data_to_send=}")
            logger.debug(f"{json.dumps(data_to_send)=}")
            self.kafka_produce_handler.send(
                topic=self.topic,
                data=json.dumps(data_to_send),
            )

            if self.buffer:
                logger.debug("Storing earlier messages in buffer...")
                if self.center_timestamp:
                    self.begin_timestamp = self.center_timestamp
                    logger.debug(f"begin_timestamp set to former center_timestamp: {self.begin_timestamp}")
                self.earlier_messages = self.latest_messages
                self.center_timestamp = self.end_timestamp
                logger.debug(f"center_timestamp set to former end_timestamp: {self.center_timestamp}")
                self.end_timestamp = None
                logger.debug(f"end_timestamp set to {self.end_timestamp}")
                logger.debug("Earlier messages stored in buffer.")

            self.latest_messages = []
            logger.info("Batch successfully sent.")
        else:
            logger.debug("Messages are empty. Nothing to send.")

        logger.debug("Calling _reset_timer()...")
        self._reset_timer()

    def _get_new_batch_data_packet_to_send(self, key: str) -> dict:
        latest_batch = self.latest_messages.get_messages(key)

        begin_timestamp = self.latest_messages.begin_timestamp[key]
        end_timestamp = current_time()

        return {
            "begin_timestamp": begin_timestamp,
            "end_timestamp": end_timestamp,
            "data": latest_batch,
        }

    def _get_full_data_packet_to_send(self, key: str) -> dict:
        earlier_batch = self.earlier_messages.get_messages(key)
        latest_batch = self.latest_messages.get_messages(key)

        begin_timestamp = self.earlier_messages.begin_timestamp[key]
        end_timestamp = current_time()

        return {
            "begin_timestamp": begin_timestamp,
            "end_timestamp": end_timestamp,
            "data": earlier_batch + latest_batch,
        }

    def _send_data_packet(self, key: str, data: dict) -> None:
        logger.debug("Sending data to KafkaProduceHandler...")
        logger.debug(f"{data=}")
        self.kafka_produce_handler.send(
            topic=self.topic,
            data=json.dumps(data),
        )
        logger.debug(f"{data=}")

    def _move_to_buffer(self, key: str, end_timestamp: str) -> None:
        self.earlier_messages.begin_timestamp[key] = self.latest_messages.begin_timestamp[key]
        self.earlier_messages.end_timestamp[key] = end_timestamp

        self.latest_messages.remove_messages(key)

    def _reset_timer(self) -> None:
        """
        Resets an existing or starts a new timer with the globally set batch timeout. In the case of a timeout,
        the batch is sent.
        """
        logger.debug("Resetting timer...")
        if self.timer:
            logger.debug("Cancelling active timer...")
            self.timer.cancel()
        else:
            logger.debug("No timer active.")

        logger.debug("Starting new timer...")
        self.timer = Timer(BATCH_TIMEOUT, self._send_batch_for_key)
        self.timer.start()
        logger.debug("Successfully started new timer.")


# TODO: Test
class CollectorKafkaBatchSender(KafkaBatchSender):
    """
    Specific type of :class:`KafkaBatchSender` used in the :class:`LogCollector`. Calls the standard
    :class:`KafkaBatchSender` with arguments ``topic="Prefilter", transactional_id=transactional_id, buffer=True``,
    fitting the :class:`LogCollector` usage. Stores a batch of incoming data messages and sends the entire batch as
    single message to the respective :class:`KafkaHandler`. Batch is sent when it is full or the timer runs out.
    """

    def __init__(self, transactional_id: str) -> None:
        """
        The batch is kept until the next batch reaches the maximum number of elements. Then, both batches are sent as
        concatenation. The older batch is removed and the same procedure repeats with the next batch.

        Args:
            transactional_id (str): ID of the transaction to send the full batch with
        """
        logger.debug("Calling KafkaBatchSender(topic='Prefilter', transactional_id=transactional_id, buffer=True)...")
        super().__init__(topic="Prefilter", transactional_id=transactional_id, buffer=True)
        logger.debug(f"Initialized CollectorKafkaBatchSender ({transactional_id=}).")
