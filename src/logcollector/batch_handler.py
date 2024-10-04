import json
import logging
import os
import sys
from datetime import datetime
from threading import Timer

from src.base.kafka_handler import KafkaProduceHandler
from src.base.utils import setup_config

sys.path.append(os.getcwd())
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

config = setup_config()
BATCH_SIZE = config["kafka"]["batch_sender"]["batch_size"]
BATCH_TIMEOUT = config["kafka"]["batch_sender"]["batch_timeout"]


class BufferedBatch:
    """
    Data structure for managing the batch, buffer, and timestamps. The batch contains the latest messages and a
    buffer that stores the previous batch messages. Also sorts the batches and can return timestamps.
    """

    def __init__(self):
        self.batch = {}  # Batch for the latest messages coming in
        self.buffer = {}  # Former batch with previous messages

    def add_message(self, key: str, message: str) -> None:
        """
        Adds a given message to the messages list of the given key. If the key already exists, the message is simply
        added, otherwise, the key is created.

        Args:
            key (str): Key to which the message is added
            message (str): Message to be added
        """
        if key in self.batch:  # key already has messages associated
            self.batch[key].append(message)
            logger.debug(f"Message '{message}' added to {key}'s batch.")
        else:  # key has no messages associated yet
            self.batch[key] = [message]
            logger.debug(f"Message '{message}' added to newly created {key}'s batch.")

    def get_number_of_messages(self, key: str) -> int:
        """
        Returns the number of entries in the batch of the latest messages.

        Args:
            key (str): Key for which to return the number of messages

        Returns:
            Number of messages associated with the given key as integer
        """
        if key in self.batch:
            return len(self.batch[key])

        return 0

    def get_number_of_buffered_messages(self, key: str) -> int:
        """
        Returns the number of entries in the buffer of the latest messages.

        Args:
            key (str): Key for which to return the number of messages in the buffer

        Returns:
            Number of messages in buffer associated with the given key as integer
        """
        if key in self.buffer:
            return len(self.buffer[key])

        return 0

    @staticmethod
    def sort_messages(
        data: list[tuple[str, str]], timestamp_format: str = "%Y-%m-%dT%H:%M:%S.%fZ"
    ) -> list[str]:
        """
        Sorts the given list of loglines by their respective timestamps, in ascending order.

        Args:
            timestamp_format (str): Format of the timestamps
            data (list[tuple[str, str]]): List of loglines to be sorted, with the tuple of strings consisting of 1. the
                                          timestamps of the message and 2. the full message (unchanged, i.e. including
                                          the respective timestamp)

        Returns:
            List of log lines as strings sorted by timestamps (ascending)
        """
        sorted_data = sorted(
            data, key=lambda x: datetime.strptime(x[0], timestamp_format)
        )
        loglines = [message for _, message in sorted_data]

        return loglines

    @staticmethod
    def extract_tuples_from_json_formatted_strings(
        data: list[str],
    ) -> list[tuple[str, str]]:
        """
        Args:
            data (list[str]): Input list of strings to be prepared

        Returns:
            Tuple with timestamps and log lines, which is needed for :func:'sort_messages'.
        """
        tuples = []

        for item in data:
            record = json.loads(item)

            timestamp = record.get("timestamp", "")
            tuples.append((str(timestamp), item))

        return tuples

    def get_first_timestamp_of_buffer(self, key: str) -> str | None:
        """
        Get the first timestamp of the buffer messages list.

        Returns:
            First timestamp of the list of buffer messages for the given key, None if the key's list is empty
        """
        entries = self.buffer.get(key)

        return json.loads(entries[0])["timestamp"] if entries and entries[0] else None

    def get_last_timestamp_of_buffer(self, key: str) -> str | None:
        """
        Get the last timestamp of the buffer messages list.

        Returns:
            Last timestamp of the list of buffer messages for the given key, None if the key's list is empty
        """
        entries = self.buffer.get(key)

        return json.loads(entries[-1])["timestamp"] if entries and entries[-1] else None

    def get_first_timestamp_of_batch(self, key: str) -> str | None:
        """
        Get the first timestamp of the batch messages list.

        Returns:
            First timestamp of the list of batch messages for the given key, None if the key's list is empty
        """
        entries = self.batch.get(key)

        return json.loads(entries[0])["timestamp"] if entries and entries[0] else None

    def get_last_timestamp_of_batch(self, key: str) -> str | None:
        """
        Get the last timestamp of the batch messages list.

        Returns:
            Last timestamp of the list of batch messages for the given key, None if the key's list is empty
        """
        entries = self.batch.get(key)

        return json.loads(entries[-1])["timestamp"] if entries and entries[-1] else None

    def sort_buffer(self, key: str):
        """
        Sorts the buffer's messages by their timestamp, in ascending order.

        Args:
            key (str): Key for which to sort entries
        """
        if key in self.buffer:
            self.buffer[key] = self.sort_messages(
                self.extract_tuples_from_json_formatted_strings(self.buffer[key])
            )

    def sort_batch(self, key: str):
        """
        Sorts the batches messages by their timestamp, in ascending order.

        Args:
            key (str): Key for which to sort entries
        """
        if key in self.batch:
            self.batch[key] = self.sort_messages(
                self.extract_tuples_from_json_formatted_strings(self.batch[key])
            )

    def complete_batch(self, key: str) -> dict:
        """
        Completes the batch and returns a full data packet including timestamps and messages. Depending on the
        stored data, either both batches are added to the packet, or just the latest messages. Raises a ValueError
        if no data is available.

        Args:
            key (str): Key for which to complete the current batch and return data packet

        Returns:
            Dictionary of begin_timestamp, end_timestamp and messages (including buffered data) associated with a key

        Raises:
            ValueError: No data is available for sending.
        """
        if self.batch.get(key):
            if not self.buffer.get(key):  # Variant 1: Only batch has entries
                logger.debug("Variant 1: Only batch has entries. Sending...")
                self.sort_batch(key)
                buffer_data = []
                begin_timestamp = self.get_first_timestamp_of_batch(key)
            else:  # Variant 2: Buffer and batch have entries
                logger.debug("Variant 2: Buffer and batch have entries. Sending...")
                self.sort_batch(key)
                self.sort_buffer(key)
                buffer_data = self.buffer[key]
                begin_timestamp = self.get_first_timestamp_of_buffer(key)

            data = {
                "begin_timestamp": begin_timestamp,
                "end_timestamp": self.get_last_timestamp_of_batch(key),
                "data": buffer_data + self.batch[key],
            }

            # Move data from batch to buffer
            self.buffer[key] = self.batch[key]
            del self.batch[key]

            return data

        if self.buffer:  # Variant 3: Only buffer has entries
            logger.debug("Variant 3: Only buffer has entries.")
            logger.debug(
                "Deleting buffer data (has no influence on analysis since it was too long ago)..."
            )
            del self.buffer[key]
        else:  # Variant 4: No data exists
            logger.debug("Variant 4: No data exists. Nothing to send.")

        raise ValueError("No data available for sending.")

    def get_stored_keys(self) -> set:
        """
        Retrieve all keys stored in either the batch or the buffer.

        Returns:
            List of all keys stored in either dictionary
        """
        keys_set = set()

        for key in self.batch:
            keys_set.add(key)

        for key in self.buffer:
            keys_set.add(key)

        return keys_set.copy()


class CollectorKafkaBatchSender:
    """
    Adds messages to the :class:`BufferedBatch` and sends them after a timer ran out or the respective batch is full.
    """

    def __init__(self):
        self.topic = "Prefilter"
        self.batch = BufferedBatch()
        self.timer = None

        logger.debug(f"Calling KafkaProduceHandler(transactional_id='collector')...")
        self.kafka_produce_handler = KafkaProduceHandler(transactional_id="collector")
        logger.debug(f"Initialized KafkaBatchSender.")

    def __del__(self):
        logger.debug(f"Closing KafkaBatchSender ({self.topic=})...")
        if self.timer:
            logger.debug("Timer is active. Cancelling timer...")
            self.timer.cancel()
            logger.debug("Timer cancelled.")

        self._send_all_batches(reset_timer=False)

        logger.debug(f"Closed KafkaBatchSender ({self.topic=}).")

    def add_message(self, key: str, message: str) -> None:
        """
        Adds the given message to the key specific batch. Checks if the batch is full. If so, it is sent. In the first
        execution, the timer starts. The same timer is used for all keys.

        Args:
            message (str): Message to be added to the batch
            key (str): Key of the message (e.g. subnet of client IP address in a log message)
        """
        logger.debug(f"Adding message '{message}' to batch.")

        self.batch.add_message(key, message)

        logger.debug(f"Batch: {self.batch.batch}")
        number_of_messages_for_key = self.batch.get_number_of_messages(key)

        if number_of_messages_for_key >= BATCH_SIZE:
            logger.debug(
                f"Batch for {key=} is full. Calling _send_batch_for_key({key})..."
            )
            self._send_batch_for_key(key)
            logger.info(
                f"Full batch: Successfully sent batch messages for subnet_id {key}.\n"
                f"    ⤷  {number_of_messages_for_key} messages sent."
            )
        elif not self.timer:  # First time setting the timer
            logger.debug("Timer not set yet. Calling _reset_timer()...")
            self._reset_timer()

        logger.debug(f"Message '{message}' successfully added to batch for {key=}.")

    def _send_all_batches(self, reset_timer: bool = True) -> None:
        number_of_keys = 0
        total_number_of_batch_messages = 0
        total_number_of_buffer_messages = 0

        for key in self.batch.get_stored_keys():
            number_of_keys += 1
            total_number_of_batch_messages += self.batch.get_number_of_messages(key)
            total_number_of_buffer_messages += (
                self.batch.get_number_of_buffered_messages(key)
            )
            self._send_batch_for_key(key)

        if reset_timer:
            self._reset_timer()

        if not total_number_of_batch_messages:
            return

        if number_of_keys == 1:
            logger.info(
                "Successfully sent all batches.\n"
                f"    ⤷  Batch for one subnet_id with "
                f"{total_number_of_batch_messages + total_number_of_buffer_messages} message(s) sent ("
                f"{total_number_of_batch_messages} batch message(s), {total_number_of_buffer_messages} "
                f"buffer message(s))."
            )
        else:  # if number_of_keys > 1
            logger.info(
                "Successfully sent all batches.\n"
                f"    ⤷  Batches for {number_of_keys} subnet_ids sent. "
                f"In total: {total_number_of_batch_messages + total_number_of_buffer_messages} messages ("
                f"{total_number_of_batch_messages} batch message(s), {total_number_of_buffer_messages} "
                f"buffer message(s))"
            )

    def _send_batch_for_key(self, key: str) -> None:
        logger.debug(f"Starting to send the batch for {key=}...")

        try:
            data_packet = self.batch.complete_batch(key)
        except ValueError as e:
            logger.debug(e)
            return

        self._send_data_packet(key, data_packet)

    def _send_data_packet(self, key: str, data: dict) -> None:
        logger.debug("Sending data to KafkaProduceHandler...")
        logger.debug(f"{data=}")
        self.kafka_produce_handler.send(
            topic=self.topic,
            data=json.dumps(data),
            key=key,
        )
        logger.debug(f"{data=}")

    def _reset_timer(self) -> None:
        """
        Resets an existing or starts a new timer with the globally set batch timeout. In the case of a timeout,
        all batches are sent. The timer serves as a backup so that all batches are cleared sometimes (prevents subnets
        with few messages from never being analyzed).
        """
        logger.debug("Resetting timer...")
        if self.timer:
            logger.debug("Cancelling active timer...")
            self.timer.cancel()
        else:
            logger.debug("No timer active.")

        logger.debug("Starting new timer...")
        self.timer = Timer(BATCH_TIMEOUT, self._send_all_batches)
        self.timer.start()
        logger.debug("Successfully started new timer.")
